use mirajazz::{
    device::Device,
    error::MirajazzError,
    state::{DeviceStateReader, DeviceStateUpdate},
    types::HidDeviceInfo,
};
use rumqttc::AsyncClient;
use std::{collections::HashMap, time::Duration};
use tokio::sync::{broadcast, watch};

use crate::{
    config::DeviceConfig,
    icons::{self, Icon},
    mappings::{Kind, Screen},
    mqtt::{self, Command, MqttEvent},
};

/// Drives a single device from connect to disconnect. Anything that goes wrong with one device
/// is reported and ends only its own session, so the other connected devices keep running.
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
    shutdown: watch::Receiver<bool>,
) {
    println!(
        "[{serial}] Connecting to {} {} ({:04X}:{:04X})",
        kind.manufacturer(),
        kind.model(),
        info.vendor_id,
        info.product_id
    );

    let device = match connect(&info, kind).await {
        Ok(device) => device,
        Err(e) => {
            println!("[{serial}] Failed to connect: {e}");

            return;
        }
    };

    println!(
        "[{serial}] Connected. Key count: {} ({} with a screen), encoder count: {}",
        kind.key_count(),
        kind.screen_key_count(),
        kind.encoder_count()
    );

    mqtt::publish_discovery(&mqtt_client, &serial, kind).await;

    let session = session(
        &device,
        kind,
        &serial,
        &config,
        &mqtt_client,
        &mut mqtt_events,
        shutdown,
    );

    if let Err(e) = session.await {
        println!("[{serial}] Session ended: {e}");
    }

    // Best effort: when the device was unplugged it is already gone and these just fail
    let _ = device.flush().await;
    let _ = device.shutdown().await;

    println!("[{serial}] Stopped");
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
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), MirajazzError> {
    // Some devices ignore every other command until they are put into the right mode
    if let Some(mode) = kind.startup_mode() {
        device.set_mode(mode).await?;
    }

    device.set_brightness(config.brightness).await?;
    device.clear_all_button_images().await?;

    // Write the configured images to the device
    set_images(device, kind, serial, config, mqtt_client).await?;

    // Flush
    device.flush().await?;

    // The screens dim on their own, and are lit at the configured brightness, until Home Assistant
    // says otherwise, which a retained command on the subscription below does as soon as it arrives
    let (timeout, mut timeout_changes) = watch::channel(true);
    let (brightness, mut brightness_changes) = watch::channel(config.brightness);
    mqtt::publish_timeout_state(mqtt_client, serial, true).await;
    mqtt::publish_brightness_state(mqtt_client, serial, config.brightness).await;

    // Only ask for the commands Home Assistant has retained once the configured images are on the
    // device, so they arrive afterwards and take over rather than being overwritten
    mqtt::subscribe_commands(mqtt_client, serial).await;

    let reader = device.get_reader(kind.process_input());

    let input = input_loop(
        device,
        &reader,
        serial,
        config,
        mqtt_client,
        &mut timeout_changes,
        &mut brightness_changes,
    );
    let commands = command_loop(
        device,
        kind,
        serial,
        mqtt_client,
        mqtt_events,
        &timeout,
        &brightness,
    );

    tokio::select! {
        result = input => result,
        result = commands => result,
        result = keepalive_loop(device, kind) => result,
        _ = cancelled(&mut shutdown) => Ok(()),
    }
}

/// Carries out what Home Assistant asks of the device for as long as it is connected: the images
/// to put on the screens it addresses, whether those screens may dim, and how brightly they are lit
async fn command_loop(
    device: &Device,
    kind: Kind,
    serial: &str,
    mqtt_client: &AsyncClient,
    events: &mut broadcast::Receiver<MqttEvent>,
    timeout: &watch::Sender<bool>,
    brightness: &watch::Sender<u8>,
) -> Result<(), MirajazzError> {
    use broadcast::error::RecvError;

    loop {
        match events.recv().await {
            Ok(MqttEvent::Message(publish)) => {
                // Every device sees every message, so most of them are somebody else's
                match mqtt::command(&publish, serial) {
                    Some(Command::Image(screen, payload)) => {
                        set_image(device, kind, serial, mqtt_client, screen, payload).await?;
                    }
                    Some(Command::Timeout(enabled)) => {
                        set_timeout(serial, mqtt_client, timeout, enabled).await;
                    }
                    Some(Command::Brightness(percent)) => {
                        set_brightness(serial, mqtt_client, brightness, percent).await;
                    }
                    None => {}
                }
            }
            Ok(MqttEvent::Connected) => {
                // A reconnected session has none of our subscriptions, and a broker restarted
                // without persistence has none of our discovery configs or states either
                let enabled = *timeout.borrow();
                let percent = *brightness.borrow();

                mqtt::publish_discovery(mqtt_client, serial, kind).await;
                mqtt::publish_timeout_state(mqtt_client, serial, enabled).await;
                mqtt::publish_brightness_state(mqtt_client, serial, percent).await;
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
async fn input_loop(
    device: &Device,
    reader: &DeviceStateReader,
    serial: &str,
    config: &DeviceConfig,
    mqtt_client: &AsyncClient,
    timeout_changes: &mut watch::Receiver<bool>,
    brightness_changes: &mut watch::Receiver<u8>,
) -> Result<(), MirajazzError> {
    use tokio::time::{Duration, Instant, timeout_at};

    // We dim after `timeout` seconds of inactivity, unless the timeout is switched off.
    let idle_timeout = Duration::from_secs(config.timeout);
    let mut is_dimmed = false;
    let mut idle_deadline = Instant::now() + idle_timeout;
    // The screens are already lit at this, `session` set it before handing over
    let mut brightness = config.brightness;

    loop {
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
            enabled = changed(timeout_changes) => {
                if enabled {
                    // A timeout just switched on counts the idle period from now, rather than
                    // dimming straight away because the device had been sitting idle
                    idle_deadline = Instant::now() + idle_timeout;
                } else if is_dimmed && wake(device, serial, brightness).await {
                    is_dimmed = false;
                }

                continue;
            }
            percent = changed(brightness_changes) => {
                brightness = percent;

                // Screens that have dimmed stay dim: the new level is what they come back to on
                // the next input, rather than a slider lighting up a device nobody has touched
                if !is_dimmed && let Err(e) = device.set_brightness(brightness).await {
                    println!("[{serial}] Failed to set brightness: {e}");
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
                    match device.sleep().await {
                        Ok(()) => is_dimmed = true,
                        Err(e) => println!("[{serial}] Failed to dim brightness: {e}"),
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
        if is_dimmed && wake(device, serial, brightness).await {
            is_dimmed = false;
        }

        idle_deadline = Instant::now() + idle_timeout;

        // Event handler
        for update in updates {
            match update {
                DeviceStateUpdate::ButtonDown(i) => {
                    mqtt::handle_button(mqtt_client, serial, i).await;
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

/// Brings the screens back up after they dimmed, and says whether they are lit now, so a device
/// that wouldn't come back is tried again on the next input rather than left dim for good
async fn wake(device: &Device, serial: &str, brightness: u8) -> bool {
    match device.set_brightness(brightness).await {
        Ok(()) => true,
        Err(e) => {
            println!("[{serial}] Failed to restore brightness: {e}");

            false
        }
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

/// Pings devices that drop the connection when idle. Never returns for devices that don't
/// need it, so it can always be selected over.
async fn keepalive_loop(device: &Device, kind: Kind) -> Result<(), MirajazzError> {
    let Some(interval) = kind.keepalive_interval() else {
        return std::future::pending().await;
    };

    loop {
        tokio::time::sleep(interval).await;

        device.keep_alive().await?;
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
    device: &Device,
    kind: Kind,
    serial: &str,
    mqtt_client: &AsyncClient,
    screen: Screen,
    payload: &[u8],
) -> Result<(), MirajazzError> {
    let Some((hw_key, format)) = kind.resolve_screen(screen) else {
        report_missing_screen(serial, kind, screen);

        return Ok(());
    };

    // Decoding a picture someone else chose the size of is the one slow, purely CPU-bound step
    // in here, so get it off the async worker, the same way mirajazz does when it converts an
    // image for the device
    let icon = tokio::task::block_in_place(|| icons::from_payload(payload));

    let state = match icon {
        Ok(Icon::Show { image, state }) => {
            device.set_button_image(hw_key, format, image).await?;

            state
        }
        Ok(Icon::Clear) => {
            device.clear_button_image(hw_key).await?;

            String::new()
        }
        // Home Assistant handing our own marker back, which says nothing about what to show
        Ok(Icon::Unchanged) => return Ok(()),
        Err(e) => {
            println!("[{serial}] Ignoring image for {screen}: {e}");

            return Ok(());
        }
    };

    device.flush().await?;

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
