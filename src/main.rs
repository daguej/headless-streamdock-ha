use image::open;
use mirajazz::{
    device::{Device, list_devices},
    error::MirajazzError,
    state::DeviceStateUpdate,
    types::HidDeviceInfo,
};
use std::time::Duration;
use tokio::signal::unix::{SignalKind, signal};

use crate::{
    config::Config,
    mappings::{Kind, QUERIES},
};

mod config;
mod inputs;
mod mappings;
mod mqtt;

#[tokio::main]
async fn main() -> Result<(), MirajazzError> {
    // SIGINT = Ctrl+C
    let mut sigint = signal(SignalKind::interrupt()).unwrap();
    // SIGTERM = kill or systemd stop
    let mut sigterm = signal(SignalKind::terminate()).unwrap();

    let (mqtt_client, mut mqtt_eventloop) = mqtt::init_client("headless-streamdock");
    // rumqttc requires the eventloop to be polled continuously to drive the connection.
    let mqtt_poll_handle = tokio::spawn(async move {
        loop {
            if let Err(e) = mqtt_eventloop.poll().await {
                println!("MQTT connection error: {e}");
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }
        }
    });

    let config = config::load_config().expect("Failed to load config");

    let devices = list_devices(QUERIES).await?;

    if devices.is_empty() {
        println!("No supported devices found, check that the udev rules are installed");
    }

    for dev in devices {
        // The queries only match known devices, so this is mostly here to get the kind
        let Some(kind) = Kind::from_vid_pid(dev.vendor_id, dev.product_id) else {
            println!(
                "Ignoring unsupported device {:04X}:{:04X}",
                dev.vendor_id, dev.product_id
            );

            continue;
        };

        println!(
            "Connecting to {} {} ({:04X}:{:04X})",
            kind.manufacturer(),
            kind.model(),
            dev.vendor_id,
            dev.product_id
        );

        // Connect to the device
        let device = connect(&dev, kind).await?;

        // Print out some info from the device
        println!("Connected to '{}'", device.serial_number());

        let device_id = device.serial_number().to_string();
        mqtt::publish_discovery(&mqtt_client, &device_id, kind).await;

        // Some devices ignore every other command until they are put into the right mode
        if let Some(mode) = kind.startup_mode() {
            device.set_mode(mode).await?;
        }

        // Track brightness so we can dim on inactivity and restore on activity.
        let current_brightness: u8 = config.brightness;
        device.set_brightness(current_brightness).await?;
        device.clear_all_button_images().await?;

        println!(
            "Key count: {} ({} with a screen), encoder count: {}",
            kind.key_count(),
            kind.screen_key_count(),
            kind.encoder_count()
        );

        // Write the configured images to the device
        set_images(&device, kind, &config).await?;

        // Flush
        device.flush().await?;

        let reader = device.get_reader(kind.process_input());

        let main_loop = async {
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
                        println!("Ignoring unrecognized report from device");
                        continue;
                    }
                    Ok(Err(e)) => {
                        println!("Failed to read from device: {e}");
                        break;
                    }
                    Err(_) => {
                        if !is_dimmed {
                            if let Err(e) = device.sleep().await {
                                println!("Failed to dim brightness: {e}");
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
                    if let Err(e) = device.set_brightness(current_brightness).await {
                        println!("Failed to restore brightness: {e}");
                    } else {
                        is_dimmed = false;
                    }
                }

                idle_deadline = Instant::now() + idle_timeout;

                // Event handler
                for update in updates {
                    match update {
                        DeviceStateUpdate::ButtonDown(i) => {
                            mqtt::handle_button(&mqtt_client, &device_id, i).await;
                        }
                        DeviceStateUpdate::EncoderTwist(i, value) => {
                            mqtt::handle_knob(&mqtt_client, &device_id, i, value).await;
                        }
                        DeviceStateUpdate::EncoderDown(i) => {
                            mqtt::handle_knob_press(&mqtt_client, &device_id, i).await;
                        }
                        _ => {}
                    }
                }
            }
        };

        // Ensure controlled exit
        let mut shutting_down = false;

        tokio::select! {
            _ = main_loop => {},
            _ = keepalive_loop(&device, kind) => {},
            _ = sigint.recv() => {
                shutting_down = true;
                println!("Received SIGINT")
            },
            _ = sigterm.recv() => {
                shutting_down = true;
                println!("Received SIGTERM")
            }
        }

        println!("Exiting...");

        drop(reader);

        device.flush().await?;
        device.shutdown().await?;

        if shutting_down {
            break;
        }
    }

    mqtt_poll_handle.abort();
    mqtt_client.disconnect().await.ok();

    Ok(())
}

/// Connects to a device, retrying while it looks like udev hasn't caught up with it yet,
/// which happens when this program starts at boot together with the device
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
async fn set_images(device: &Device, kind: Kind, config: &Config) -> Result<(), MirajazzError> {
    for button in &config.buttons {
        if button.id as usize >= kind.screen_key_count() {
            println!(
                "Ignoring icon for button {}: {} {} only has screens on {} buttons",
                button.id,
                kind.manufacturer(),
                kind.model(),
                kind.screen_key_count()
            );

            continue;
        }

        device
            .set_button_image(button.id, kind.image_format(), load_icon(&button.icon))
            .await?;
    }

    for segment in &config.lcd {
        let Some(hw_key) = kind.lcd_hw_key(segment.id) else {
            println!(
                "Ignoring icon for LCD segment {}: {} {} has {} LCD segments",
                segment.id,
                kind.manufacturer(),
                kind.model(),
                kind.lcd_segment_count()
            );

            continue;
        };

        device
            .set_button_image(hw_key, kind.lcd_image_format(), load_icon(&segment.icon))
            .await?;
    }

    Ok(())
}

fn load_icon(icon: &str) -> image::DynamicImage {
    open(format!("images/{icon}")).unwrap_or_else(|_| panic!("Failed to open image {icon}"))
}

/// Pings devices that drop the connection when idle. Never returns for devices that don't
/// need it, so it can always be selected over.
async fn keepalive_loop(device: &Device, kind: Kind) {
    let Some(interval) = kind.keepalive_interval() else {
        std::future::pending::<()>().await;

        return;
    };

    loop {
        tokio::time::sleep(interval).await;

        if let Err(e) = device.keep_alive().await {
            println!("Keepalive failed: {e}");

            return;
        }
    }
}
