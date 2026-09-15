use mirajazz::{
    device::Device,
    error::MirajazzError,
    state::{DeviceStateReader, DeviceStateUpdate},
    types::HidDeviceInfo,
};
use rumqttc::AsyncClient;
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    time::Duration,
};
use tokio::sync::{Mutex, broadcast, watch};

use crate::{
    config::DeviceConfig,
    icons::{self, Icon},
    mappings::{Kind, Screen},
    mqtt::{self, Command, MqttEvent},
};

/// How long to wait before reconnecting a device whose session ended on its own
const RECONNECT_DELAY: Duration = Duration::from_secs(1);

/// How far that wait is allowed to grow while a device keeps failing as soon as it connects, so a
/// device we can't get anything out of is retried occasionally instead of spun on
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(30);

/// A session that lasted this long was a working device that hit a one-off rather than one that
/// never got going, so it starts the wait above over again
const HEALTHY_SESSION: Duration = Duration::from_secs(60);

/// How long to let a device act on a mode change before saying anything else to it
const MODE_SETTLE: Duration = Duration::from_millis(50);

/// Everything we ask a device to do is a few kilobytes at most, which is nothing; taking longer
/// than this means it is dragging its heels over accepting the data rather than us being slow to
/// send it. Worth saying out loud, because the kernel gives up on a report that takes more than
/// five seconds and that ends the session.
const SLOW_OPERATION: Duration = Duration::from_millis(500);

/// How long a button in multi-click mode waits after being released for another press, before it
/// reports the ones it has counted
const MULTI_CLICK_WINDOW: Duration = Duration::from_millis(400);

/// Carries out one thing we ask of a device, reporting it when it fails or drags.
///
/// Every write here is a blocking transfer the kernel abandons after five seconds, and when that
/// happens the only thing distinguishing the commands is which one we were in the middle of. So
/// each is named, and the time it took is reported either way: a command that fails says how long
/// it hung on for first, which is what separates a device refusing data from one never asked.
async fn timed<T>(
    serial: &str,
    what: &str,
    op: impl Future<Output = Result<T, MirajazzError>>,
) -> Result<T, MirajazzError> {
    let started = tokio::time::Instant::now();
    let result = op.await;
    let took = started.elapsed();
    let seconds = took.as_secs_f32();

    match &result {
        Err(e) => println!("[{serial}] {what} failed after {seconds:.1}s: {e}"),
        Ok(_) if took >= SLOW_OPERATION => println!("[{serial}] {what} took {seconds:.1}s"),
        Ok(_) => {}
    }

    result
}

/// Drives a single device for as long as it stays plugged in. Anything that goes wrong with one
/// device is reported and ends only its own session, so the other connected devices keep running.
///
/// A session that ends on its own is reconnected rather than left for dead: the device is still
/// plugged in, so the watcher has no event to report for it, and nothing else would ever bring it
/// back. Unplugging is what ends this for good, either because the watcher aborts us or because
/// the device stops being found below.
///
/// The serial number doubles as the device's identity everywhere: it keys the config, names the
/// MQTT topics, and prefixes this device's log lines.
pub async fn run(
    info: HidDeviceInfo,
    kind: Kind,
    serial: String,
    config: DeviceConfig,
    mqtt_client: AsyncClient,
    mut mqtt_events: broadcast::Receiver<MqttEvent>,
    mut shutdown: watch::Receiver<bool>,
) {
    println!(
        "[{serial}] Connecting to {} {} ({:04X}:{:04X})",
        kind.manufacturer(),
        kind.model(),
        info.vendor_id,
        info.product_id
    );

    let mut delay = RECONNECT_DELAY;

    loop {
        let outcome = attempt(
            &info,
            kind,
            &serial,
            &config,
            &mqtt_client,
            &mut mqtt_events,
            &mut shutdown,
        )
        .await;

        let Attempt::Failed { was_working } = outcome else {
            break;
        };

        // A device that had been working and hit a one-off is worth coming straight back to; one
        // that fails as soon as we connect to it is backed off rather than spun on
        if was_working {
            delay = RECONNECT_DELAY;
        }

        // Don't announce a reconnect we aren't going to make
        if *shutdown.borrow() {
            break;
        }

        println!("[{serial}] Reconnecting in {}s", delay.as_secs());

        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            // No point sitting out the wait when we're shutting down anyway
            _ = cancelled(&mut shutdown) => break,
        }

        delay = (delay * 2).min(MAX_RECONNECT_DELAY);
    }

    println!("[{serial}] Stopped");
}

/// What one go at running a device came to, and so whether it is worth another
enum Attempt {
    /// The device is gone, or we were asked to stop: there is nothing left to do for it
    Finished,
    /// It failed, but the device is still plugged in, so it can be reconnected. Says whether it
    /// had been working for a while first, which decides how long we wait before doing that.
    Failed { was_working: bool },
}

/// Connects to a device and runs it until the session ends, one way or another
async fn attempt(
    info: &HidDeviceInfo,
    kind: Kind,
    serial: &str,
    config: &DeviceConfig,
    mqtt_client: &AsyncClient,
    mqtt_events: &mut broadcast::Receiver<MqttEvent>,
    shutdown: &mut watch::Receiver<bool>,
) -> Attempt {
    let device = match connect(info, kind).await {
        Ok(device) => device,
        // The backend no longer finding the device at all is how an unplug reaches us when the
        // watcher isn't around to report it. Anything else is a device that is still plugged in,
        // and a device that is there is always worth another try.
        Err(MirajazzError::DeviceNotFoundError) => {
            println!("[{serial}] Unplugged");

            return Attempt::Finished;
        }
        Err(e) => {
            println!("[{serial}] Failed to connect: {e}");

            return Attempt::Failed { was_working: false };
        }
    };

    println!(
        "[{serial}] Connected. Key count: {} ({} with a screen), encoder count: {}",
        kind.key_count(),
        kind.screen_key_count(),
        kind.encoder_count()
    );

    mqtt::publish_discovery(mqtt_client, serial, kind).await;

    let started = tokio::time::Instant::now();
    let result = session(
        &device,
        kind,
        serial,
        config,
        mqtt_client,
        mqtt_events,
        shutdown,
    )
    .await;

    // Best effort: when the device was unplugged it is already gone and these just fail
    let _ = device.flush().await;
    let _ = device.shutdown().await;

    // Let go of the device before the caller waits to reconnect: opening it afresh is the whole
    // point of trying again, and there is nothing left here to keep it open for
    drop(device);

    // Being asked to stop is the only way a session ends without something having gone wrong
    let Err(e) = result else {
        return Attempt::Finished;
    };

    // How long it lasted is the first thing worth knowing: a session that dies on the same command
    // every time looks nothing like one that ran for a while and then hit something
    println!(
        "[{serial}] Session ended after {:.1}s: {e}",
        started.elapsed().as_secs_f32()
    );

    Attempt::Failed {
        was_working: started.elapsed() >= HEALTHY_SESSION,
    }
}

/// How the device's screens are lit, shared by the loops that run it.
///
/// All three are watch channels rather than plain values because no one loop owns them: the input
/// loop dims the screens and wakes them on the next input, the command loop wakes them to show an
/// image, and Home Assistant changes the two settings from underneath both. Whoever changes one
/// has to be sure the others find out.
struct Backlight {
    /// Whether the screens may dim once the device has been left alone, or are to be kept lit
    timeout: watch::Sender<bool>,
    /// How brightly the screens are lit while they are awake
    brightness: watch::Sender<u8>,
    /// Whether the screens are asleep right now
    dimmed: watch::Sender<bool>,
}

/// Sets the device up and then reads from it until it goes away or we're asked to shut down.
/// The reader is dropped when this returns, so the caller can still talk to the device.
async fn session(
    device: &Device,
    kind: Kind,
    serial: &str,
    config: &DeviceConfig,
    mqtt_client: &AsyncClient,
    mqtt_events: &mut broadcast::Receiver<MqttEvent>,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<(), MirajazzError> {
    // Nothing else is talking to the device until the loops below start, so the setup here has it
    // to itself and needs no lock

    // Some devices ignore every other command until they are put into the right mode
    if let Some(mode) = kind.startup_mode() {
        timed(serial, "Setting the mode", device.set_mode(mode)).await?;

        // A mode change takes the device a moment to act on, and it ignores what arrives in the
        // meantime. mirajazz's own N1 example waits the same way before saying anything else.
        tokio::time::sleep(MODE_SETTLE).await;
    }

    timed(
        serial,
        "Setting the startup brightness",
        device.set_brightness(config.brightness),
    )
    .await?;
    timed(
        serial,
        "Blanking the screens",
        device.clear_all_button_images(),
    )
    .await?;

    // Write the configured images to the device
    set_images(device, kind, serial, config, mqtt_client).await?;

    // Flush
    timed(serial, "Sending the startup images", device.flush()).await?;

    // The screens dim on their own, and are lit at the configured brightness, until Home Assistant
    // says otherwise, which a retained command on the subscription below does as soon as it arrives
    let backlight = Backlight {
        timeout: watch::Sender::new(true),
        brightness: watch::Sender::new(config.brightness),
        dimmed: watch::Sender::new(false),
    };

    // Every button reports each press straight away until Home Assistant switches it to counting
    // them. Shared the same way the backlight is: the command loop changes it, the input loop acts
    // on it.
    let multi_click = watch::Sender::new(HashSet::new());

    mqtt::publish_timeout_state(mqtt_client, serial, true).await;
    mqtt::publish_brightness_state(mqtt_client, serial, config.brightness).await;
    mqtt::publish_multi_click_states(mqtt_client, serial, kind, &HashSet::new()).await;

    // Only ask for the commands Home Assistant has retained once the configured images are on the
    // device, so they arrive afterwards and take over rather than being overwritten
    mqtt::subscribe_commands(mqtt_client, serial).await;

    let reader = device.get_reader(kind.process_input());

    // mirajazz hands out the device's writer one report at a time, so an operation that takes
    // several of them lets go of it in between: an image is a header saying how many bytes
    // follow, then those bytes in 1KB reports. All three loops below write to this device, and a
    // report from one of them landing inside another's transfer leaves the device waiting on
    // bytes that never come, out of step with us until it stops acknowledging writes at all —
    // which surfaces as a write timing out. Holding this for a whole operation is what keeps them
    // out of each other's transfers.
    //
    // Reading is deliberately not covered by it: input comes from the device's other half, and
    // waiting for a keypress under this lock would stop everything else from writing.
    let device = Mutex::new(device);

    let input = input_loop(
        &device,
        &reader,
        serial,
        config,
        mqtt_client,
        &backlight,
        &multi_click,
    );
    let commands = command_loop(
        &device,
        kind,
        serial,
        mqtt_client,
        mqtt_events,
        &backlight,
        &multi_click,
    );

    tokio::select! {
        result = input => result,
        result = commands => result,
        result = keepalive_loop(&device, serial, kind) => result,
        _ = cancelled(shutdown) => Ok(()),
    }
}

/// Carries out what Home Assistant asks of the device for as long as it is connected: the images
/// to put on the screens it addresses, which buttons count their presses, whether those screens may
/// dim, and how brightly they are lit
async fn command_loop(
    device: &Mutex<&Device>,
    kind: Kind,
    serial: &str,
    mqtt_client: &AsyncClient,
    events: &mut broadcast::Receiver<MqttEvent>,
    backlight: &Backlight,
    multi_click: &watch::Sender<HashSet<u8>>,
) -> Result<(), MirajazzError> {
    use broadcast::error::RecvError;

    loop {
        match events.recv().await {
            Ok(MqttEvent::Message(publish)) => {
                // Every device sees every message, so most of them are somebody else's
                match mqtt::command(&publish, serial) {
                    Some(Command::Image(screen, payload)) => {
                        set_image(
                            device,
                            kind,
                            serial,
                            mqtt_client,
                            backlight,
                            screen,
                            payload,
                        )
                        .await?;
                    }
                    Some(Command::Timeout(enabled)) => {
                        set_timeout(serial, mqtt_client, &backlight.timeout, enabled).await;
                    }
                    Some(Command::Brightness(percent)) => {
                        set_brightness(serial, mqtt_client, &backlight.brightness, percent).await;
                    }
                    Some(Command::MultiClick(id, enabled)) => {
                        set_multi_click(kind, serial, mqtt_client, multi_click, id, enabled).await;
                    }
                    None => {}
                }
            }
            Ok(MqttEvent::Connected) => {
                // A reconnected session has none of our subscriptions, and a broker restarted
                // without persistence has none of our discovery configs or states either
                let enabled = *backlight.timeout.borrow();
                let percent = *backlight.brightness.borrow();
                let counting = multi_click.borrow().clone();

                mqtt::publish_discovery(mqtt_client, serial, kind).await;
                mqtt::publish_timeout_state(mqtt_client, serial, enabled).await;
                mqtt::publish_brightness_state(mqtt_client, serial, percent).await;
                mqtt::publish_multi_click_states(mqtt_client, serial, kind, &counting).await;
                mqtt::subscribe_commands(mqtt_client, serial).await;
            }
            // Images are big, so only a few are kept waiting. Dropping the ones this device
            // couldn't keep up with is better than holding on to megabytes of stale pictures.
            Err(RecvError::Lagged(n)) => {
                println!("[{serial}] Dropped {n} MQTT message(s) that arrived too fast to draw");
            }
            // The event loop is only gone once the whole app is shutting down. The device stays
            // usable as an input until it is told to stop, so don't end the session over it.
            Err(RecvError::Closed) => return std::future::pending().await,
        }
    }
}

/// Reads input for as long as the device keeps talking to us, dimming the screens after the
/// configured period of inactivity and restoring them on the next input.
///
/// While the timeout is switched off the screens are left lit however long the device sits
/// untouched, and switching it off wakes screens that have already dimmed. This loop also owns how
/// bright the screens are lit, because it is the one that dims and wakes them.
///
/// It isn't the only one that wakes them, though: the command loop does too, to show an image on a
/// device that had gone to sleep. `dimmed` is how the two keep the same idea of what state the
/// screens are in, and how this loop hears about a wake it didn't do itself.
///
/// A button reports each press as it goes down, unless it is one of those in `multi_click`. Those
/// count their releases instead, and report how many there were once `MULTI_CLICK_WINDOW` passes
/// without another.
async fn input_loop(
    device: &Mutex<&Device>,
    reader: &DeviceStateReader,
    serial: &str,
    config: &DeviceConfig,
    mqtt_client: &AsyncClient,
    backlight: &Backlight,
    multi_click: &watch::Sender<HashSet<u8>>,
) -> Result<(), MirajazzError> {
    use tokio::time::{Duration, Instant, sleep_until, timeout_at};

    // We dim after `timeout` seconds of inactivity, unless the timeout is switched off.
    let idle_timeout = Duration::from_secs(config.timeout);
    let mut idle_deadline = Instant::now() + idle_timeout;
    // The screens are already lit at this, `session` set it before handing over
    let mut brightness = config.brightness;

    let dimmed = &backlight.dimmed;
    let mut dim_changes = dimmed.subscribe();
    let mut timeout_changes = backlight.timeout.subscribe();
    let mut brightness_changes = backlight.brightness.subscribe();

    // Buttons that went down while they were counting their presses. Whether a release counts is
    // decided by this rather than by the mode at the time of the release, so a button switched
    // over while it is held neither reports the same press twice nor loses it.
    let mut held = HashSet::new();
    // The presses counted so far for every button still waiting to see if another follows, and
    // when it stops waiting. A button that is down again has no such time: it can't be done
    // counting until it is released, however long it is held.
    let mut presses: HashMap<u8, (u32, Option<Instant>)> = HashMap::new();

    loop {
        // The soonest any button is done counting, copied out so the counts can change below
        let presses_due = presses.values().filter_map(|&(_, due)| due).min();

        // Read back rather than remembered, because the command loop wakes the screens too
        let is_dimmed = *dim_changes.borrow_and_update();

        // While the screens are lit and the timeout is on, stop waiting at the idle deadline
        // so we can dim. Once dimmed, or with the timeout off, there is nothing to wait for
        // but the next input, or for the setting to change under us.
        let dims = !is_dimmed && *timeout_changes.borrow_and_update();

        let result = tokio::select! {
            // Input that has already been read is worth more than being prompt about a setting
            // that is only ever changed by hand, so never drop a read in favour of a change
            biased;

            result = async {
                if dims {
                    timeout_at(idle_deadline, reader.read(None)).await
                } else {
                    Ok(reader.read(None).await)
                }
            } => result,
            enabled = changed(&mut timeout_changes) => {
                if enabled {
                    // A timeout just switched on counts the idle period from now, rather than
                    // dimming straight away because the device had been sitting idle
                    idle_deadline = Instant::now() + idle_timeout;
                } else if is_dimmed {
                    wake(*device.lock().await, serial, dimmed, brightness).await;
                }

                continue;
            }
            percent = changed(&mut brightness_changes) => {
                brightness = percent;

                // Screens that have dimmed stay dim: the new level is what they come back to on
                // the next input, rather than a slider lighting up a device nobody has touched
                if !is_dimmed {
                    let device = device.lock().await;
                    let _ = timed(
                        serial,
                        "Changing the brightness",
                        device.set_brightness(brightness),
                    )
                    .await;
                }

                continue;
            }
            // Someone else changed what state the screens are in, which only the command loop
            // waking them to show an image does
            still_dimmed = changed(&mut dim_changes) => {
                if !still_dimmed {
                    // The screens are lit again, so they get the full idle period from here
                    // before this loop dims them a second time
                    idle_deadline = Instant::now() + idle_timeout;
                }

                continue;
            }
            // A button has gone long enough without another press to report the ones it counted
            _ = async {
                match presses_due {
                    Some(due) => sleep_until(due).await,
                    None => std::future::pending().await,
                }
            } => {
                let now = Instant::now();
                let done: Vec<_> = presses
                    .iter()
                    .filter(|&(_, &(_, due))| due.is_some_and(|due| due <= now))
                    .map(|(&i, &(count, _))| (i, count))
                    .collect();

                for (i, count) in done {
                    presses.remove(&i);
                    mqtt::handle_button(mqtt_client, serial, i, count).await;
                }

                continue;
            }
        };

        let updates = match result {
            Ok(Ok(updates)) => updates,
            // A report we couldn't decode isn't worth giving up over
            Ok(Err(MirajazzError::BadData)) => {
                println!("[{serial}] Ignoring unrecognized report from device");
                continue;
            }
            Ok(Err(e)) => return Err(e),
            Err(_) => {
                if !is_dimmed {
                    let device = device.lock().await;

                    if timed(serial, "Dimming the screens", device.sleep())
                        .await
                        .is_ok()
                    {
                        dimmed.send_replace(true);
                    }
                }
                continue;
            }
        };

        // Devices also send frames that aren't input at all (the N1 has a periodic
        // status frame), and those shouldn't count as activity.
        if updates.is_empty() {
            continue;
        }

        // We got some updates: ensure brightness is restored if we were dimmed.
        if is_dimmed {
            wake(*device.lock().await, serial, dimmed, brightness).await;
        }

        idle_deadline = Instant::now() + idle_timeout;

        // Event handler
        for update in updates {
            match update {
                DeviceStateUpdate::ButtonDown(i) => {
                    let counting = multi_click.borrow().contains(&i);

                    if counting {
                        held.insert(i);

                        // Pressed again in time, so hold off reporting until this press is
                        // released too, rather than letting the window run out while it is down
                        if let Some((_, due)) = presses.get_mut(&i) {
                            *due = None;
                        }
                    } else {
                        // Presses counted before the button stopped counting them happened first,
                        // so they are reported first
                        if let Some((count, _)) = presses.remove(&i) {
                            mqtt::handle_button(mqtt_client, serial, i, count).await;
                        }

                        mqtt::handle_button(mqtt_client, serial, i, 1).await;
                    }
                }
                DeviceStateUpdate::ButtonUp(i) => {
                    if held.remove(&i) {
                        // Every release gives the button the whole window again to be pressed once
                        // more
                        let count = presses.get(&i).map_or(0, |&(count, _)| count);
                        let due = Instant::now() + MULTI_CLICK_WINDOW;

                        presses.insert(i, (count + 1, Some(due)));
                    }
                }
                DeviceStateUpdate::EncoderTwist(i, value) => {
                    mqtt::handle_knob(mqtt_client, serial, i, value).await;
                }
                DeviceStateUpdate::EncoderDown(i) => {
                    mqtt::handle_knob_press(mqtt_client, serial, i).await;
                }
                _ => {}
            }
        }
    }
}

/// Brings the screens back up after they dimmed. The device is only recorded as lit once it says
/// the brightness took, so one that wouldn't come back is tried again rather than left dim for
/// good while everyone believes it is awake.
///
/// Takes the device rather than the lock around it, because every caller is already holding it.
async fn wake(device: &Device, serial: &str, dimmed: &watch::Sender<bool>, brightness: u8) {
    let lit = timed(
        serial,
        "Waking the screens",
        device.set_brightness(brightness),
    )
    .await;

    if lit.is_ok() {
        dimmed.send_replace(false);
    }
}

/// Yields a setting every time it changes, and never again once nothing can change it, so it can
/// always be selected over
async fn changed<T: Clone>(setting: &mut watch::Receiver<T>) -> T {
    if setting.changed().await.is_err() {
        return std::future::pending().await;
    }

    setting.borrow_and_update().clone()
}

/// Switches the screen timeout on or off, and reports it back. The input loop is what acts on it:
/// it owns both the idle deadline and the brightness.
async fn set_timeout(
    serial: &str,
    mqtt_client: &AsyncClient,
    timeout: &watch::Sender<bool>,
    enabled: bool,
) {
    // A command that says what the setting already is happens on every reconnect, when the broker
    // replays the retained one, and isn't worth waking the input loop or Home Assistant for
    let changed = timeout.send_if_modified(|current| {
        let changed = *current != enabled;
        *current = enabled;

        changed
    });

    if !changed {
        return;
    }

    println!(
        "[{serial}] Screen timeout {}",
        if enabled { "enabled" } else { "disabled" }
    );

    mqtt::publish_timeout_state(mqtt_client, serial, enabled).await;
}

/// Sets how bright the screens are lit, and reports it back. Like the timeout, the input loop is
/// what acts on it: it owns the brightness, because it is the one that dims and wakes the screens.
async fn set_brightness(
    serial: &str,
    mqtt_client: &AsyncClient,
    brightness: &watch::Sender<u8>,
    percent: u8,
) {
    // A command that says what the brightness already is happens on every reconnect, when the
    // broker replays the retained one, and isn't worth redrawing the screens over
    let changed = brightness.send_if_modified(|current| {
        let changed = *current != percent;
        *current = percent;

        changed
    });

    if !changed {
        return;
    }

    println!("[{serial}] Screen brightness set to {percent}%");

    mqtt::publish_brightness_state(mqtt_client, serial, percent).await;
}

/// Switches a button between reporting each press as it goes down and counting its presses, and
/// reports it back. The input loop is what acts on it, from the next time the button goes down.
async fn set_multi_click(
    kind: Kind,
    serial: &str,
    mqtt_client: &AsyncClient,
    multi_click: &watch::Sender<HashSet<u8>>,
    id: u8,
    enabled: bool,
) {
    if id as usize >= kind.key_count() {
        println!(
            "[{serial}] Ignoring multi-click setting for button {id}: the {} {} has {} buttons",
            kind.manufacturer(),
            kind.model(),
            kind.key_count()
        );

        return;
    }

    // A command that says what the setting already is happens on every reconnect, when the broker
    // replays the retained one, and isn't worth telling Home Assistant about again
    let changed = multi_click.send_if_modified(|buttons| {
        if enabled {
            buttons.insert(id)
        } else {
            buttons.remove(&id)
        }
    });

    if !changed {
        return;
    }

    println!(
        "[{serial}] Multi-click {} for {}",
        if enabled { "enabled" } else { "disabled" },
        kind.button_label(id)
    );

    mqtt::publish_multi_click_state(mqtt_client, serial, kind, id, enabled).await;
}

/// Pings devices that drop the connection when idle. Never returns for devices that don't
/// need it, so it can always be selected over.
async fn keepalive_loop(
    device: &Mutex<&Device>,
    serial: &str,
    kind: Kind,
) -> Result<(), MirajazzError> {
    let Some(interval) = kind.keepalive_interval() else {
        return std::future::pending().await;
    };

    loop {
        tokio::time::sleep(interval).await;

        let device = device.lock().await;

        timed(serial, "The keepalive ping", device.keep_alive()).await?;
    }
}

/// Resolves once we've been asked to shut down, including when the request came in before this
/// device's task got going or when the sender is already gone
async fn cancelled(shutdown: &mut watch::Receiver<bool>) {
    while !*shutdown.borrow_and_update() {
        if shutdown.changed().await.is_err() {
            return;
        }
    }
}

/// Connects to a device, retrying while it looks like udev hasn't caught up with it yet,
/// which happens both when this program starts at boot together with the device and when a
/// device is hotplugged
async fn connect(dev: &HidDeviceInfo, kind: Kind) -> Result<Device, MirajazzError> {
    const MAX_ATTEMPTS: u8 = 10;
    const RETRY_DELAY: Duration = Duration::from_millis(300);

    for attempt in 1..=MAX_ATTEMPTS {
        let error = match Device::connect(
            dev,
            kind.protocol_version(),
            kind.key_count(),
            kind.encoder_count(),
        )
        .await
        {
            Ok(device) => return Ok(device),
            Err(error) => error,
        };

        let message = error.to_string();
        let retryable = message.contains("Permission denied")
            || message.contains("Resource busy")
            || message.contains("Disconnected");

        if !retryable || attempt == MAX_ATTEMPTS {
            return Err(error);
        }

        println!("Connect attempt {attempt}/{MAX_ATTEMPTS} failed: {message}, retrying");
        tokio::time::sleep(RETRY_DELAY).await;
    }

    unreachable!("the loop above returns on the last attempt");
}

/// Writes the images from the config to the screens the device actually has, and reports what
/// every one of them is showing, so Home Assistant starts out in step with the device.
///
/// The changes only reach the screens once they are flushed.
async fn set_images(
    device: &Device,
    kind: Kind,
    serial: &str,
    config: &DeviceConfig,
    mqtt_client: &AsyncClient,
) -> Result<(), MirajazzError> {
    let configured = configured_icons(kind, serial, config);

    for screen in kind.screens() {
        // `screens` only lists what this model has, so all of them resolve
        let Some((hw_key, format)) = kind.resolve_screen(screen) else {
            continue;
        };

        let state = match configured.get(&screen) {
            // Every screen was blanked just before this, so one with no icon is already right
            None => String::new(),
            Some(name) => match icons::from_file(name) {
                Ok(image) => {
                    device.set_button_image(hw_key, format, image).await?;

                    (*name).to_string()
                }
                // An icon that can't be read leaves one screen blank rather than taking the
                // whole device down
                Err(e) => {
                    println!("[{serial}] Leaving {screen} blank: {e}");

                    String::new()
                }
            },
        };

        mqtt::publish_image_state(mqtt_client, serial, screen, &state).await;
    }

    Ok(())
}

/// Indexes the config's icons by the screen they belong to. The same config file is meant to
/// work with either model, so an entry for a screen this device doesn't have is reported and
/// left out rather than treated as an error.
fn configured_icons<'a>(
    kind: Kind,
    serial: &str,
    config: &'a DeviceConfig,
) -> HashMap<Screen, &'a str> {
    let buttons = config
        .buttons
        .iter()
        .map(|button| (Screen::Button(button.id), button.icon.as_str()));
    let lcd = config
        .lcd
        .iter()
        .map(|segment| (Screen::Lcd(segment.id), segment.icon.as_str()));

    let mut icons = HashMap::new();

    for (screen, name) in buttons.chain(lcd) {
        if kind.resolve_screen(screen).is_none() {
            report_missing_screen(serial, kind, screen);

            continue;
        }

        icons.insert(screen, name);
    }

    icons
}

/// Draws one image sent by Home Assistant, and reports back what the screen is showing now.
/// A command that doesn't make sense is reported and leaves the screen as it was, rather than
/// ending the session.
async fn set_image(
    device: &Mutex<&Device>,
    kind: Kind,
    serial: &str,
    mqtt_client: &AsyncClient,
    backlight: &Backlight,
    screen: Screen,
    payload: &[u8],
) -> Result<(), MirajazzError> {
    let Some((hw_key, format)) = kind.resolve_screen(screen) else {
        report_missing_screen(serial, kind, screen);

        return Ok(());
    };

    // Decoding a picture someone else chose the size of is the one slow, purely CPU-bound step
    // in here, so get it off the async worker, the same way mirajazz does when it converts an
    // image for the device. Decoded before taking the device below, so the other loops aren't
    // held up by it.
    let icon = tokio::task::block_in_place(|| icons::from_payload(payload));

    // What to put on the screen, and what to report back that it is showing. Decided before the
    // device is touched, because the commands that turn out to want nothing from it are the
    // common case and mustn't wake it.
    let (image, state) = match icon {
        Ok(Icon::Show { image, state }) => (Some(image), state),
        Ok(Icon::Clear) => (None, String::new()),
        // Home Assistant handing our own marker back, which says nothing about what to show, and
        // so is no reason to light a device up
        Ok(Icon::Unchanged) => return Ok(()),
        Err(e) => {
            println!("[{serial}] Ignoring image for {screen}: {e}");

            return Ok(());
        }
    };

    // Held across all of it: the flush is where the image is actually transferred, and it is that
    // transfer the other loops must not write into
    let device = device.lock().await;

    // A dimmed device is asleep, and a sleeping screen is in no state to be drawn on: it takes the
    // picture slowly if at all, which is how a transfer ends up timing out. Wake it first, and let
    // the input loop dim it again once it has been left alone for a while. Sent even when we
    // believe the screens are lit, since it is only the brightness they are already at, and a
    // device that dozed off on its own is worth waking anyway.
    let level = *backlight.brightness.borrow();
    wake(*device, serial, &backlight.dimmed, level).await;

    // Timed apart on purpose. Converting an image is pure CPU and touches nothing, while the flush
    // is every byte of it going down the wire, so which of the two drags says whether a slow screen
    // is the machine we're running on or the device we're talking to.
    match image {
        Some(image) => {
            timed(
                serial,
                &format!("Converting the image for {screen}"),
                device.set_button_image(hw_key, format, image),
            )
            .await?
        }
        None => {
            timed(
                serial,
                &format!("Blanking {screen}"),
                device.clear_button_image(hw_key),
            )
            .await?
        }
    }

    timed(
        serial,
        &format!("Sending {screen} to the device"),
        device.flush(),
    )
    .await?;

    drop(device);

    mqtt::publish_image_state(mqtt_client, serial, screen, &state).await;

    Ok(())
}

fn report_missing_screen(serial: &str, kind: Kind, screen: Screen) {
    println!(
        "[{serial}] Ignoring image for {screen}: the {} {} has {} button screens and {} LCD segments",
        kind.manufacturer(),
        kind.model(),
        kind.screen_key_count(),
        kind.lcd_segment_count()
    );
}
