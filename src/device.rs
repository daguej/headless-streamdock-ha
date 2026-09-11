use image::{DynamicImage, open};
use mirajazz::{
    device::Device,
    error::MirajazzError,
    state::{DeviceStateReader, DeviceStateUpdate},
    types::HidDeviceInfo,
};
use rumqttc::AsyncClient;
use std::time::Duration;
use tokio::sync::watch;

use crate::{config::DeviceConfig, mappings::Kind, mqtt};

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

    if let Err(e) = session(&device, kind, &serial, &config, &mqtt_client, shutdown).await {
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
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), MirajazzError> {
    // Some devices ignore every other command until they are put into the right mode
    if let Some(mode) = kind.startup_mode() {
        device.set_mode(mode).await?;
    }

    device.set_brightness(config.brightness).await?;
    device.clear_all_button_images().await?;

    // Write the configured images to the device
    set_images(device, kind, serial, config).await?;

    // Flush
    device.flush().await?;

    let reader = device.get_reader(kind.process_input());

    tokio::select! {
        result = input_loop(device, &reader, serial, config, mqtt_client) => result,
        result = keepalive_loop(device, kind) => result,
        _ = cancelled(&mut shutdown) => Ok(()),
    }
}

/// Reads input for as long as the device keeps talking to us, dimming the screens after the
/// configured period of inactivity and restoring them on the next input
async fn input_loop(
    device: &Device,
    reader: &DeviceStateReader,
    serial: &str,
    config: &DeviceConfig,
    mqtt_client: &AsyncClient,
) -> Result<(), MirajazzError> {
    use tokio::time::{Duration, Instant, timeout_at};

    // We dim after `timeout` seconds of inactivity.
    let idle_timeout = Duration::from_secs(config.timeout);
    let mut is_dimmed = false;
    let mut idle_deadline = Instant::now() + idle_timeout;

    loop {
        // While the screens are lit, stop waiting at the idle deadline so we can dim.
        // Once dimmed there is nothing to wait for but the next input.
        let result = if is_dimmed {
            Ok(reader.read(None).await)
        } else {
            timeout_at(idle_deadline, reader.read(None)).await
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
                    if let Err(e) = device.sleep().await {
                        println!("[{serial}] Failed to dim brightness: {e}");
                    } else {
                        is_dimmed = true;
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
            if let Err(e) = device.set_brightness(config.brightness).await {
                println!("[{serial}] Failed to restore brightness: {e}");
            } else {
                is_dimmed = false;
            }
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

/// Writes the images from the config to the buttons and LCD segments the device actually has
async fn set_images(
    device: &Device,
    kind: Kind,
    serial: &str,
    config: &DeviceConfig,
) -> Result<(), MirajazzError> {
    for button in &config.buttons {
        if button.id as usize >= kind.screen_key_count() {
            println!(
                "[{serial}] Ignoring icon for button {}: {} {} only has screens on {} buttons",
                button.id,
                kind.manufacturer(),
                kind.model(),
                kind.screen_key_count()
            );

            continue;
        }

        let Some(icon) = load_icon(&button.icon, serial) else {
            continue;
        };

        device
            .set_button_image(button.id, kind.image_format(), icon)
            .await?;
    }

    for segment in &config.lcd {
        let Some(hw_key) = kind.lcd_hw_key(segment.id) else {
            println!(
                "[{serial}] Ignoring icon for LCD segment {}: {} {} has {} LCD segments",
                segment.id,
                kind.manufacturer(),
                kind.model(),
                kind.lcd_segment_count()
            );

            continue;
        };

        let Some(icon) = load_icon(&segment.icon, serial) else {
            continue;
        };

        device
            .set_button_image(hw_key, kind.lcd_image_format(), icon)
            .await?;
    }

    Ok(())
}

/// An icon that can't be read leaves one button blank rather than taking the whole device down
fn load_icon(icon: &str, serial: &str) -> Option<DynamicImage> {
    match open(format!("images/{icon}")) {
        Ok(image) => Some(image),
        Err(e) => {
            println!("[{serial}] Failed to open image {icon}: {e}");

            None
        }
    }
}
