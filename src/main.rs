use futures_lite::{Stream, StreamExt};
use mirajazz::{
    device::{DeviceWatcher, list_devices},
    types::{DeviceLifecycleEvent, HidDeviceInfo},
};
use rumqttc::AsyncClient;
use std::{collections::HashMap, error::Error, time::Duration};
use tokio::{
    signal::unix::{SignalKind, signal},
    sync::{broadcast, watch},
    task::{AbortHandle, JoinSet},
};

use crate::{
    config::Config,
    mappings::{Kind, QUERIES},
    mqtt::MqttEvent,
};

mod config;
mod device;
mod icons;
mod inputs;
mod mappings;
mod mqtt;

/// How long devices get to put their screens to sleep before we stop waiting for them
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // SIGINT = Ctrl+C
    let mut sigint = signal(SignalKind::interrupt())?;
    // SIGTERM = kill or systemd stop
    let mut sigterm = signal(SignalKind::terminate())?;

    let config = config::load_config().expect("Failed to load config");

    let (mqtt_client, mqtt_eventloop) = mqtt::init_client("headless-streamdock");
    let (mqtt_events, mqtt_poll_handle) = mqtt::spawn_event_loop(mqtt_eventloop);

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut devices = Devices::new(config, mqtt_client.clone(), mqtt_events, shutdown_rx);

    // Start watching before enumerating, so a device plugged in while we are starting up is
    // picked up by one or the other. Attaching is idempotent, so showing up in both is fine.
    let mut watcher = DeviceWatcher::new();
    let mut events = match watcher.watch(QUERIES).await {
        Ok(events) => Some(events),
        Err(e) => {
            println!(
                "Hotplug detection unavailable ({e}), only devices connected now will be used"
            );

            None
        }
    };

    for dev in list_devices(QUERIES).await? {
        devices.attach(dev.to_device_info());
    }

    if devices.is_empty() {
        println!("No supported devices connected, check that the udev rules are installed");

        if events.is_some() {
            println!("Waiting for one to be plugged in");
        }
    }

    loop {
        tokio::select! {
            event = next_event(&mut events) => match event {
                DeviceLifecycleEvent::Connected(info) => devices.attach(info),
                DeviceLifecycleEvent::Disconnected(info) => devices.detach(&info),
            },
            // A device task reconnects its own device, so one that stopped on its own has given
            // up on it (an unplug the watcher hasn't reported yet) and has to be forgotten, so
            // the device can be attached again if it comes back
            Some(_) = devices.join_next() => devices.reap(),
            _ = sigint.recv() => {
                println!("Received SIGINT");
                break;
            }
            _ = sigterm.recv() => {
                println!("Received SIGTERM");
                break;
            }
        }
    }

    println!("Exiting...");

    // Let the devices shut their screens down before we tear the process down
    shutdown_tx.send_replace(true);
    devices.shutdown().await;

    mqtt_poll_handle.abort();
    mqtt_client.disconnect().await.ok();

    Ok(())
}

/// Yields the next hotplug event, and never resolves once there are no more to come, so it
/// can always be selected over
async fn next_event<S>(events: &mut Option<S>) -> DeviceLifecycleEvent
where
    S: Stream<Item = DeviceLifecycleEvent> + Unpin,
{
    let event = match events {
        Some(events) => events.next().await,
        None => None,
    };

    match event {
        Some(event) => event,
        None => std::future::pending().await,
    }
}

/// Keeps one task per connected device, so every device runs independently of the others
struct Devices {
    config: Config,
    mqtt_client: AsyncClient,
    /// Incoming MQTT, which every device listens to for the messages addressed to it
    mqtt_events: broadcast::Sender<MqttEvent>,
    shutdown: watch::Receiver<bool>,
    /// Serial number of every device we are currently running, and the handle to stop it
    running: HashMap<String, AbortHandle>,
    tasks: JoinSet<()>,
}

impl Devices {
    fn new(
        config: Config,
        mqtt_client: AsyncClient,
        mqtt_events: broadcast::Sender<MqttEvent>,
        shutdown: watch::Receiver<bool>,
    ) -> Self {
        Self {
            config,
            mqtt_client,
            mqtt_events,
            shutdown,
            running: HashMap::new(),
            tasks: JoinSet::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.running.is_empty()
    }

    /// Starts running a newly seen device. Devices we can't identify, and ones already
    /// running, are ignored.
    fn attach(&mut self, info: HidDeviceInfo) {
        // A device whose task has already ended is not running anymore, even if we haven't
        // got round to joining it yet, so don't let a stale entry block it from coming back
        self.reap();

        // The queries only match known devices, so this is mostly here to get the kind
        let Some(kind) = Kind::from_vid_pid(info.vendor_id, info.product_id) else {
            println!(
                "Ignoring unsupported device {:04X}:{:04X}",
                info.vendor_id, info.product_id
            );

            return;
        };

        // Every supported device reports a serial, and we need one: it's how Home Assistant
        // and the config tell two otherwise identical docks apart
        let Some(serial) = info.serial_number.clone() else {
            println!(
                "Ignoring {} {} that reports no serial number",
                kind.manufacturer(),
                kind.model()
            );

            return;
        };

        // The same device can be reported twice, by the startup enumeration and by the
        // watcher, and a device can expose more than one matching interface
        if self.running.contains_key(&serial) {
            return;
        }

        let handle = self.tasks.spawn(device::run(
            info,
            kind,
            serial.clone(),
            self.config.for_device(&serial),
            self.mqtt_client.clone(),
            self.mqtt_events.subscribe(),
            self.shutdown.clone(),
        ));

        self.running.insert(serial, handle);
    }

    /// Stops running an unplugged device. There is no point shutting it down gracefully,
    /// it's already gone.
    fn detach(&mut self, info: &HidDeviceInfo) {
        let Some(serial) = info.serial_number.as_deref() else {
            return;
        };

        if let Some(handle) = self.running.remove(serial) {
            println!("[{serial}] Unplugged");

            handle.abort();
        }
    }

    async fn join_next(&mut self) -> Option<()> {
        self.tasks.join_next().await.map(|_| ())
    }

    /// Forgets the devices whose tasks have ended, so they can be attached again if they
    /// come back
    fn reap(&mut self) {
        self.running.retain(|_, handle| !handle.is_finished());
    }

    /// Waits for the running devices to finish shutting down, but not forever: a device that
    /// stopped responding shouldn't hold up the exit
    async fn shutdown(mut self) {
        let drain = async { while self.tasks.join_next().await.is_some() {} };

        if tokio::time::timeout(SHUTDOWN_GRACE, drain).await.is_err() {
            println!("Giving up on devices that didn't shut down in time");

            self.tasks.shutdown().await;
        }
    }
}
