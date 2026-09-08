use dotenv::dotenv;
use rumqttc::{AsyncClient, EventLoop, MqttOptions, QoS};
use serde_json::json;
use std::{collections::HashMap, env::var, time::Duration};

use crate::config;

const MANUFACTURER: &str = "Mirabox";
const MODEL: &str = "Stream Dock N3";

pub fn init_client(client_id: &str) -> (AsyncClient, EventLoop) {
    dotenv().ok();

    let host: String = var("MQTT_HOST").expect("MQTT_HOST env variable is missing");
    let port: u16 = var("MQTT_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(1883);

    let mut mqtt_options = MqttOptions::new(client_id, host, port);
    mqtt_options.set_keep_alive(Duration::from_secs(30));

    if let (Ok(username), Ok(password)) = (var("MQTT_USERNAME"), var("MQTT_PASSWORD")) {
        mqtt_options.set_credentials(username, password);
    }

    AsyncClient::new(mqtt_options, 10)
}

fn discovery_prefix() -> String {
    var("MQTT_DISCOVERY_PREFIX").unwrap_or_else(|_| "homeassistant".to_string())
}

fn trigger_topic(device_id: &str) -> String {
    format!("streamdock/{device_id}/trigger")
}

// Publishes retained HA MQTT device-automation discovery configs, one trigger per button and two (rotate_left/rotate_right) per knob.
pub async fn publish_discovery(
    client: &AsyncClient,
    device_id: &str,
    device_name: &str,
    buttons: &[config::ButtonConfig],
    knobs: &[config::KnobConfig],
) {
    let prefix = discovery_prefix();
    let topic = trigger_topic(device_id);
    let device = json!({
        "identifiers": [format!("streamdock_{device_id}")],
        "name": device_name,
        "manufacturer": MANUFACTURER,
        "model": MODEL,
    });

    for b in buttons {
        let payload = json!({
            "automation_type": "trigger",
            "platform": "device_automation",
            "topic": topic,
            "type": "button_short_press",
            "subtype": b.name,
            "payload": format!("button_{}_press", b.id),
            "device": device,
        });

        publish_discovery_config(
            client,
            &prefix,
            device_id,
            &format!("button_{}", b.id),
            &payload,
        )
        .await;
    }

    for k in knobs {
        for (direction, suffix) in [("rotate_left", "left"), ("rotate_right", "right")] {
            let payload = json!({
                "automation_type": "trigger",
                "platform": "device_automation",
                "topic": topic,
                "type": direction,
                "subtype": k.name,
                "payload": format!("knob_{}_{suffix}", k.id),
                "device": device,
            });

            let object_id = format!("knob_{}_{suffix}", k.id);
            publish_discovery_config(client, &prefix, device_id, &object_id, &payload).await;
        }
    }
}

async fn publish_discovery_config(
    client: &AsyncClient,
    prefix: &str,
    device_id: &str,
    object_id: &str,
    payload: &serde_json::Value,
) {
    let topic = format!("{prefix}/device_automation/{device_id}/{object_id}/config");
    client
        .publish(topic, QoS::AtLeastOnce, true, payload.to_string())
        .await
        .unwrap_or_else(|_| println!("Failed to publish discovery config for {object_id}"));
}

pub async fn handle_button(
    client: &AsyncClient,
    device_id: &str,
    buttons: &HashMap<u8, config::ButtonConfig>,
    i: u8,
) {
    if buttons.contains_key(&i) {
        publish_trigger(client, device_id, &format!("button_{i}_press")).await;
    } else {
        println!("No config for button {i}")
    }
}

pub async fn handle_knob(
    client: &AsyncClient,
    device_id: &str,
    knobs: &HashMap<u8, config::KnobConfig>,
    i: u8,
    value: i8,
) {
    if knobs.contains_key(&i) {
        let suffix = if value > 0 { "right" } else { "left" };
        publish_trigger(client, device_id, &format!("knob_{i}_{suffix}")).await;
    } else {
        println!("No config for knob {i}")
    }
}

async fn publish_trigger(client: &AsyncClient, device_id: &str, payload: &str) {
    client
        .publish(trigger_topic(device_id), QoS::AtLeastOnce, false, payload)
        .await
        .unwrap_or_else(|_| println!("Failed to publish trigger {payload}"));
}
