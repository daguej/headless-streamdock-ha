use dotenv::dotenv;
use rumqttc::{AsyncClient, Event, EventLoop, MqttOptions, Packet, Publish, QoS};
use serde_json::json;
use std::{env::var, time::Duration};
use tokio::{sync::broadcast, task::JoinHandle};

use crate::mappings::{Kind, Screen};

/// Root of every topic this app uses, both the ones it publishes and the ones it listens on
const TOPIC_ROOT: &str = "streamdock";

/// Images sent to a screen travel as MQTT payloads, and rumqttc's 10 KiB default is far too
/// small for one. This is the ceiling on a single incoming image: comfortably more than a
/// picture for a screen this size needs, even base64 encoded, without letting one message tie
/// up an unreasonable amount of memory.
const MAX_PACKET_SIZE: usize = 512 * 1024;

/// How many events can be waiting for the devices to pick them up. Every connected device gets
/// its own retained image for every screen at once when it subscribes, so this has room for a
/// few devices' worth; a device that still falls behind skips the messages it missed.
const EVENT_CAPACITY: usize = 64;

/// How long to wait before reconnecting after the MQTT connection fails
const RECONNECT_DELAY: Duration = Duration::from_secs(1);

/// Something that happened on the MQTT connection and that the devices need to know about
#[derive(Debug, Clone)]
pub enum MqttEvent {
    /// Connected to the broker, either for the first time or after the connection dropped.
    /// A new session starts with no subscriptions, so they have to be made again.
    Connected,
    /// A message arrived on a topic someone subscribed to
    Message(Publish),
}

pub fn init_client(client_id: &str) -> (AsyncClient, EventLoop) {
    dotenv().ok();

    let host: String = var("MQTT_HOST").expect("MQTT_HOST env variable is missing");
    let port: u16 = var("MQTT_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(1883);

    let mut mqtt_options = MqttOptions::new(client_id, host, port);
    mqtt_options.set_keep_alive(Duration::from_secs(30));
    mqtt_options.set_max_packet_size(MAX_PACKET_SIZE, MAX_PACKET_SIZE);

    if let (Ok(username), Ok(password)) = (var("MQTT_USERNAME"), var("MQTT_PASSWORD")) {
        mqtt_options.set_credentials(username, password);
    }

    AsyncClient::new(mqtt_options, 10)
}

/// Polls the connection and passes what happens on it to every device.
///
/// rumqttc needs the event loop polled continuously to drive the connection, and the devices are
/// the ones that know which topics are theirs, so this only broadcasts what it sees. Devices come
/// and go while the connection stays up, so they subscribe to the broadcast rather than being
/// registered here.
pub fn spawn_event_loop(
    mut eventloop: EventLoop,
) -> (broadcast::Sender<MqttEvent>, JoinHandle<()>) {
    let (events, _) = broadcast::channel(EVENT_CAPACITY);
    let sender = events.clone();

    let handle = tokio::spawn(async move {
        loop {
            // Sending fails only while no device is listening, which is not a problem: a
            // device subscribes to its topics as it starts, and gets the retained state then
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    let _ = sender.send(MqttEvent::Connected);
                }
                Ok(Event::Incoming(Packet::Publish(publish))) => {
                    let _ = sender.send(MqttEvent::Message(publish));
                }
                Ok(_) => {}
                Err(e) => {
                    println!("MQTT connection error: {e}");
                    tokio::time::sleep(RECONNECT_DELAY).await;
                }
            }
        }
    });

    (events, handle)
}

fn discovery_prefix() -> String {
    var("MQTT_DISCOVERY_PREFIX").unwrap_or_else(|_| "homeassistant".to_string())
}

fn trigger_topic(device_id: &str) -> String {
    format!("{TOPIC_ROOT}/{device_id}/trigger")
}

/// Where a screen's current icon is reported
fn image_topic(device_id: &str, screen: Screen) -> String {
    let path = match screen {
        Screen::Button(id) => format!("button/{id}"),
        Screen::Lcd(id) => format!("lcd/{id}"),
    };

    format!("{TOPIC_ROOT}/{device_id}/{path}/image")
}

/// Where a new icon for a screen is sent
fn image_command_topic(device_id: &str, screen: Screen) -> String {
    format!("{}/set", image_topic(device_id, screen))
}

/// Identifies a screen's entity within its device
fn image_object_id(screen: Screen) -> String {
    match screen {
        Screen::Button(id) => format!("button_{id}_image"),
        Screen::Lcd(id) => format!("lcd_{id}_image"),
    }
}

// Publishes retained HA MQTT discovery configs for one device: a device-automation trigger per
// button and three (rotate_left/rotate_right/press) per knob, plus a text entity per screen to
// set the image it shows. How many of each there are depends on the model of the device.
pub async fn publish_discovery(client: &AsyncClient, device_id: &str, kind: Kind) {
    let prefix = discovery_prefix();
    let topic = trigger_topic(device_id);
    let device = json!({
        "identifiers": [format!("{TOPIC_ROOT}_{device_id}")],
        "name": format!("Stream Dock ({device_id})"),
        "manufacturer": kind.manufacturer(),
        "model": kind.model(),
    });

    for id in 0..kind.key_count() as u8 {
        let payload = json!({
            "automation_type": "trigger",
            "platform": "device_automation",
            "topic": topic,
            "type": "button_short_press",
            "subtype": kind.button_label(id),
            "payload": format!("button_{id}_press"),
            "device": device,
        });

        publish_discovery_config(
            client,
            &prefix,
            "device_automation",
            device_id,
            &format!("button_{id}"),
            &payload,
        )
        .await;
    }

    for id in 0..kind.encoder_count() as u8 {
        for (trigger_type, suffix) in [
            ("rotate_left", "left"),
            ("rotate_right", "right"),
            ("button_short_press", "press"),
        ] {
            let payload = json!({
                "automation_type": "trigger",
                "platform": "device_automation",
                "topic": topic,
                "type": trigger_type,
                "subtype": kind.knob_label(id),
                "payload": format!("knob_{id}_{suffix}"),
                "device": device,
            });

            let object_id = format!("knob_{id}_{suffix}");
            publish_discovery_config(
                client,
                &prefix,
                "device_automation",
                device_id,
                &object_id,
                &payload,
            )
            .await;
        }
    }

    for screen in kind.screens() {
        let object_id = image_object_id(screen);
        // `retain` is what Home Assistant uses when it publishes to the command topic. Keeping
        // the command retained is what brings the screens back after either side restarts.
        let payload = json!({
            "name": format!("{screen} image"),
            "unique_id": format!("{TOPIC_ROOT}_{device_id}_{object_id}"),
            "command_topic": image_command_topic(device_id, screen),
            "state_topic": image_topic(device_id, screen),
            "retain": true,
            "entity_category": "config",
            "icon": "mdi:image",
            "device": device,
        });

        publish_discovery_config(client, &prefix, "text", device_id, &object_id, &payload).await;
    }
}

async fn publish_discovery_config(
    client: &AsyncClient,
    prefix: &str,
    component: &str,
    device_id: &str,
    object_id: &str,
    payload: &serde_json::Value,
) {
    let topic = format!("{prefix}/{component}/{device_id}/{object_id}/config");
    client
        .publish(topic, QoS::AtLeastOnce, true, payload.to_string())
        .await
        .unwrap_or_else(|_| println!("Failed to publish discovery config for {object_id}"));
}

/// Asks for the image commands addressed to one device.
///
/// This has to be repeated on every new connection, because an MQTT session starts with no
/// subscriptions. The broker replays the retained images each time, which is what restores the
/// screens after this app restarts or the connection drops.
pub async fn subscribe_images(client: &AsyncClient, device_id: &str) {
    // One filter covers every screen: the commands only differ in the button/lcd path
    let filter = format!("{TOPIC_ROOT}/{device_id}/+/+/image/set");

    if let Err(e) = client.subscribe(&filter, QoS::AtLeastOnce).await {
        println!("[{device_id}] Failed to subscribe to {filter}: {e}");
    }
}

/// Picks out the screen an incoming message addresses, and the image data it carries. Messages
/// for another device, or on a topic that isn't an image command, are not ours to handle.
pub fn image_command<'a>(publish: &'a Publish, device_id: &str) -> Option<(Screen, &'a [u8])> {
    let screen = parse_image_command(&publish.topic, device_id)?;

    Some((screen, &publish.payload))
}

fn parse_image_command(topic: &str, device_id: &str) -> Option<Screen> {
    let rest = topic
        .strip_prefix(TOPIC_ROOT)?
        .strip_prefix('/')?
        .strip_prefix(device_id)?
        .strip_prefix('/')?
        .strip_suffix("/image/set")?;

    let (part, id) = rest.split_once('/')?;
    let id = id.parse().ok()?;

    match part {
        "button" => Some(Screen::Button(id)),
        "lcd" => Some(Screen::Lcd(id)),
        _ => None,
    }
}

/// Reports what a screen is showing, so the Home Assistant entity for it matches the device.
/// Retained, so it is still right for a Home Assistant that restarts while this app keeps running.
///
/// A blank screen reports an empty payload, which subscribers receive as an empty value and which
/// also drops the retained one, leaving nothing for a screen that is showing nothing.
pub async fn publish_image_state(
    client: &AsyncClient,
    device_id: &str,
    screen: Screen,
    state: &str,
) {
    client
        .publish(
            image_topic(device_id, screen),
            QoS::AtLeastOnce,
            true,
            state,
        )
        .await
        .unwrap_or_else(|_| println!("[{device_id}] Failed to publish the state of {screen}"));
}

pub async fn handle_button(client: &AsyncClient, device_id: &str, i: u8) {
    publish_trigger(client, device_id, &format!("button_{i}_press")).await;
}

pub async fn handle_knob(client: &AsyncClient, device_id: &str, i: u8, value: i8) {
    let suffix = if value > 0 { "right" } else { "left" };
    publish_trigger(client, device_id, &format!("knob_{i}_{suffix}")).await;
}

pub async fn handle_knob_press(client: &AsyncClient, device_id: &str, i: u8) {
    publish_trigger(client, device_id, &format!("knob_{i}_press")).await;
}

async fn publish_trigger(client: &AsyncClient, device_id: &str, payload: &str) {
    client
        .publish(trigger_topic(device_id), QoS::AtLeastOnce, false, payload)
        .await
        .unwrap_or_else(|_| println!("Failed to publish trigger {payload}"));
}

#[cfg(test)]
mod tests {
    use super::*;

    const SERIAL: &str = "AL12345678";

    #[test]
    fn a_command_topic_names_the_screen_it_addresses() {
        for screen in Kind::VsdInsideN1.screens() {
            let topic = image_command_topic(SERIAL, screen);

            assert_eq!(
                parse_image_command(&topic, SERIAL),
                Some(screen),
                "{topic} did not come back as {screen}"
            );
        }
    }

    #[test]
    fn the_subscription_filter_matches_every_screens_command_topic() {
        // MQTT `+` matches exactly one level, so this only holds while every screen's topic has
        // the same shape
        for screen in Kind::VsdInsideN1.screens() {
            let topic = image_command_topic(SERIAL, screen);
            let levels: Vec<_> = topic.split('/').collect();

            assert_eq!(levels.len(), 6, "{topic} has an unexpected shape");
            assert_eq!(levels[0], TOPIC_ROOT);
            assert_eq!(levels[1], SERIAL);
            assert_eq!(&levels[4..], ["image", "set"]);
        }
    }

    #[test]
    fn the_state_topic_is_not_the_command_topic() {
        // Otherwise reporting a screen's state would immediately look like a new command
        let screen = Screen::Button(0);

        assert_ne!(
            image_topic(SERIAL, screen),
            image_command_topic(SERIAL, screen)
        );
        assert_eq!(
            parse_image_command(&image_topic(SERIAL, screen), SERIAL),
            None
        );
    }

    #[test]
    fn messages_for_another_device_are_not_ours() {
        let topic = image_command_topic("CL87654321", Screen::Button(0));

        assert_eq!(parse_image_command(&topic, SERIAL), None);
    }

    #[test]
    fn topics_that_are_not_image_commands_are_ignored() {
        for topic in [
            // The device's own trigger topic
            &trigger_topic(SERIAL),
            // A screen kind we don't know
            &format!("{TOPIC_ROOT}/{SERIAL}/dial/0/image/set"),
            // A screen id that isn't a number, or doesn't fit one
            &format!("{TOPIC_ROOT}/{SERIAL}/button/left/image/set"),
            &format!("{TOPIC_ROOT}/{SERIAL}/button/300/image/set"),
            // Missing the screen id altogether
            &format!("{TOPIC_ROOT}/{SERIAL}/button/image/set"),
            // A serial that merely starts with ours
            &format!("{TOPIC_ROOT}/{SERIAL}9/button/0/image/set"),
            // Something else entirely
            &"homeassistant/status".to_string(),
        ] {
            assert_eq!(
                parse_image_command(topic, SERIAL),
                None,
                "{topic} should not have been treated as an image command"
            );
        }
    }
}
