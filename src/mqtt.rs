use dotenv::dotenv;
use rumqttc::{AsyncClient, Event, EventLoop, MqttOptions, Packet, Publish, QoS};
use serde_json::json;
use std::{collections::HashSet, env::var, ops::RangeInclusive, time::Duration};
use tokio::{sync::broadcast, task::JoinHandle};

use crate::{
    icons::Shortened,
    mappings::{Control, Kind, MAX_PAGES, Screen},
};

/// Root of every topic this app uses, both the ones it publishes and the ones it listens on
const TOPIC_ROOT: &str = "streamdock";

/// Images sent to a screen travel as MQTT payloads, and rumqttc's 10 KiB default is far too
/// small for one. This is the ceiling on a single incoming image: comfortably more than a
/// picture for a screen this size needs, even base64 encoded, without letting one message tie
/// up an unreasonable amount of memory.
const MAX_PACKET_SIZE: usize = 512 * 1024;

/// How many events can be waiting for the devices to pick them up. Every connected device gets
/// its own retained image and multi-click setting for every button on every page at once when it
/// subscribes, so this has room for a device with every page in use; a device that still falls
/// behind skips the messages it missed.
const EVENT_CAPACITY: usize = 512;

/// How long to wait before reconnecting after the MQTT connection fails
const RECONNECT_DELAY: Duration = Duration::from_secs(1);

/// Identifies the screen timeout entity within its device
const TIMEOUT_OBJECT_ID: &str = "screen_timeout";

/// Identifies the screen brightness entity within its device
const BRIGHTNESS_OBJECT_ID: &str = "brightness";

/// The device takes its brightness as a percentage, so this is the top of the range
const MAX_BRIGHTNESS: u8 = 100;

/// Identifies the entity for the page a device is showing within its device
const PAGE_OBJECT_ID: &str = "page";

/// Identifies the entity for how many pages a device has within its device
const PAGE_COUNT_OBJECT_ID: &str = "page_count";

/// Identifies the paging mode entity within its device
const PAGING_OBJECT_ID: &str = "paging";

/// What Home Assistant's switch sends and expects by default, and what this app reports
const PAYLOAD_ON: &str = "ON";
const PAYLOAD_OFF: &str = "OFF";

/// Home Assistant's trigger types for a button or knob pressed several times in a row, by how many presses
/// each stands for. It has none past five, so a longer run is still published, but only an MQTT
/// trigger matching the payload itself (such as the blueprints use) can pick it up.
const MULTI_PRESS_TYPES: [(u32, &str); 4] = [
    (2, "button_double_press"),
    (3, "button_triple_press"),
    (4, "button_quadruple_press"),
    (5, "button_quintuple_press"),
];

/// What an incoming message asks a device to do
#[derive(Debug, PartialEq)]
pub enum Command<'a> {
    /// Show this image on one of the device's screens
    Image(Screen, &'a [u8]),
    /// Let the screens dim after the configured idle period, or keep them lit however long the
    /// device sits untouched
    Timeout(bool),
    /// Light the screens at this percentage of full brightness
    Brightness(u8),
    /// Count a button's or knob's presses in quick succession and report them together, or report
    /// every press the moment it goes down
    MultiClick(Control, bool),
    /// Show this page of buttons, counting from 0
    Page(u16),
    /// Give the device this many pages of buttons
    PageCount(u16),
    /// Have twisting a knob turn the pages instead of reporting the twist, or go back to reporting
    /// it
    Paging(bool),
}

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

/// Where it is reported whether the screens dim on their own
fn timeout_topic(device_id: &str) -> String {
    format!("{TOPIC_ROOT}/{device_id}/timeout")
}

/// Where the screen timeout is turned on and off
fn timeout_command_topic(device_id: &str) -> String {
    format!("{}/set", timeout_topic(device_id))
}

/// Where the brightness the screens are lit at is reported
fn brightness_topic(device_id: &str) -> String {
    format!("{TOPIC_ROOT}/{device_id}/brightness")
}

/// Where a new screen brightness is sent
fn brightness_command_topic(device_id: &str) -> String {
    format!("{}/set", brightness_topic(device_id))
}

/// Where it is reported whether a button or knob counts its presses
fn multi_click_topic(device_id: &str, control: Control) -> String {
    let path = match control {
        Control::Button(id) => format!("button/{id}"),
        Control::Knob(id) => format!("knob/{id}"),
    };

    format!("{TOPIC_ROOT}/{device_id}/{path}/multi_click")
}

/// Where a button's or knob's multi-click mode is turned on and off
fn multi_click_command_topic(device_id: &str, control: Control) -> String {
    format!("{}/set", multi_click_topic(device_id, control))
}

/// Where the page of buttons a device is showing is reported
fn page_topic(device_id: &str) -> String {
    format!("{TOPIC_ROOT}/{device_id}/page")
}

/// Where the page to show is sent
fn page_command_topic(device_id: &str) -> String {
    format!("{}/set", page_topic(device_id))
}

/// Where how many pages of buttons a device has is reported
fn page_count_topic(device_id: &str) -> String {
    format!("{TOPIC_ROOT}/{device_id}/page_count")
}

/// Where a new number of pages is sent
fn page_count_command_topic(device_id: &str) -> String {
    format!("{}/set", page_count_topic(device_id))
}

/// Where it is reported whether the knobs turn the pages
fn paging_topic(device_id: &str) -> String {
    format!("{TOPIC_ROOT}/{device_id}/paging")
}

/// Where paging mode is turned on and off
fn paging_command_topic(device_id: &str) -> String {
    format!("{}/set", paging_topic(device_id))
}

/// Identifies a screen's entity within its device
fn image_object_id(screen: Screen) -> String {
    match screen {
        Screen::Button(id) => format!("button_{id}_image"),
        Screen::Lcd(id) => format!("lcd_{id}_image"),
    }
}

/// Identifies a button or knob within its device, as the start of its trigger payloads and object
/// ids
fn control_object_id(control: Control) -> String {
    match control {
        Control::Button(id) => format!("button_{id}"),
        Control::Knob(id) => format!("knob_{id}"),
    }
}

/// What is published on the trigger topic when a button or knob is pressed this many times in a
/// row. A single press keeps the name it had before presses could be counted.
fn press_payload(control: Control, presses: u32) -> String {
    let object_id = control_object_id(control);

    match presses {
        1 => format!("{object_id}_press"),
        _ => format!("{object_id}_press_{presses}"),
    }
}

/// The Home Assistant device every entity and trigger of one dock belongs to
fn device_info(device_id: &str, kind: Kind) -> serde_json::Value {
    json!({
        "identifiers": [format!("{TOPIC_ROOT}_{device_id}")],
        "name": format!("Stream Dock ({device_id})"),
        "manufacturer": kind.manufacturer(),
        "model": kind.model(),
    })
}

// Publishes retained HA MQTT discovery configs for one device: three triggers
// (rotate_left/rotate_right/press) and a multi-click switch per knob, a switch for the screen
// timeout, a number for the screen brightness, numbers for how many pages there are and which one
// is showing, and a switch for whether the knobs turn those pages. Then, for each of those pages,
// everything `publish_page_discovery` publishes for its buttons and screens. How many of each there
// are depends on the model of the device.
//
// The triggers for pressing a button or knob several times in a row come and go with its
// multi-click mode, so they are published along with its state instead, by
// `publish_multi_click_states`.
pub async fn publish_discovery(client: &AsyncClient, device_id: &str, kind: Kind, pages: u16) {
    let prefix = discovery_prefix();
    let topic = trigger_topic(device_id);
    let device = device_info(device_id, kind);

    for page in 0..pages {
        publish_page_discovery(client, device_id, kind, page).await;
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

        publish_multi_click_discovery(client, &prefix, device_id, kind, &device, Control::Knob(id))
            .await;
    }

    // Retained for the same reason the images are: it is the retained command that brings the
    // setting back when either side restarts, rather than it reverting to dimming again
    let payload = json!({
        "name": "Screen timeout",
        "unique_id": format!("{TOPIC_ROOT}_{device_id}_{TIMEOUT_OBJECT_ID}"),
        "command_topic": timeout_command_topic(device_id),
        "state_topic": timeout_topic(device_id),
        "payload_on": PAYLOAD_ON,
        "payload_off": PAYLOAD_OFF,
        "retain": true,
        "entity_category": "config",
        "icon": "mdi:timer-outline",
        "device": device,
    });

    publish_discovery_config(
        client,
        &prefix,
        "switch",
        device_id,
        TIMEOUT_OBJECT_ID,
        &payload,
    )
    .await;

    // Retained as well, so the screens come back at the brightness they were last set to rather
    // than at the one in `config.toml`
    let payload = json!({
        "name": "Screen brightness",
        "unique_id": format!("{TOPIC_ROOT}_{device_id}_{BRIGHTNESS_OBJECT_ID}"),
        "command_topic": brightness_command_topic(device_id),
        "state_topic": brightness_topic(device_id),
        "min": 0,
        "max": MAX_BRIGHTNESS,
        "step": 1,
        "mode": "slider",
        "unit_of_measurement": "%",
        "retain": true,
        "entity_category": "config",
        "icon": "mdi:brightness-6",
        "device": device,
    });

    publish_discovery_config(
        client,
        &prefix,
        "number",
        device_id,
        BRIGHTNESS_OBJECT_ID,
        &payload,
    )
    .await;

    // Retained like the rest, and it is the retained command that brings the extra pages back
    let payload = json!({
        "name": "Page count",
        "unique_id": format!("{TOPIC_ROOT}_{device_id}_{PAGE_COUNT_OBJECT_ID}"),
        "command_topic": page_count_command_topic(device_id),
        "state_topic": page_count_topic(device_id),
        "min": 1,
        "max": MAX_PAGES,
        "step": 1,
        "mode": "box",
        "retain": true,
        "entity_category": "config",
        "icon": "mdi:book-multiple-outline",
        "device": device,
    });

    publish_discovery_config(
        client,
        &prefix,
        "number",
        device_id,
        PAGE_COUNT_OBJECT_ID,
        &payload,
    )
    .await;

    publish_page_number_discovery(client, device_id, kind, pages).await;

    // Retained like the rest, so a device left in paging mode is still in it after a restart
    let payload = json!({
        "name": "Paging mode",
        "unique_id": format!("{TOPIC_ROOT}_{device_id}_{PAGING_OBJECT_ID}"),
        "command_topic": paging_command_topic(device_id),
        "state_topic": paging_topic(device_id),
        "payload_on": PAYLOAD_ON,
        "payload_off": PAYLOAD_OFF,
        "retain": true,
        "entity_category": "config",
        "icon": "mdi:knob",
        "device": device,
    });

    publish_discovery_config(
        client,
        &prefix,
        "switch",
        device_id,
        PAGING_OBJECT_ID,
        &payload,
    )
    .await;
}

/// Publishes the discovery config for the number that picks the page a device shows. Its range
/// follows how many pages there are, so it is published again every time that changes.
pub async fn publish_page_number_discovery(
    client: &AsyncClient,
    device_id: &str,
    kind: Kind,
    pages: u16,
) {
    // Retained, so the device comes back on the page it was showing rather than the first one
    let payload = json!({
        "name": "Page",
        "unique_id": format!("{TOPIC_ROOT}_{device_id}_{PAGE_OBJECT_ID}"),
        "command_topic": page_command_topic(device_id),
        "state_topic": page_topic(device_id),
        "min": 0,
        "max": pages.saturating_sub(1),
        "step": 1,
        "mode": "box",
        "retain": true,
        "entity_category": "config",
        "icon": "mdi:book-open-page-variant-outline",
        "device": device_info(device_id, kind),
    });

    publish_discovery_config(
        client,
        &discovery_prefix(),
        "number",
        device_id,
        PAGE_OBJECT_ID,
        &payload,
    )
    .await;
}

/// Publishes the discovery configs for the buttons and screens on one page: a device-automation
/// trigger per button, a text entity per button screen and LCD segment to set the image it shows,
/// and a switch per button for its multi-click mode
pub async fn publish_page_discovery(client: &AsyncClient, device_id: &str, kind: Kind, page: u16) {
    let prefix = discovery_prefix();
    let topic = trigger_topic(device_id);
    let device = device_info(device_id, kind);

    for id in kind.page_buttons(page) {
        let payload = json!({
            "automation_type": "trigger",
            "platform": "device_automation",
            "topic": topic,
            "type": "button_short_press",
            "subtype": kind.button_label(id),
            "payload": press_payload(Control::Button(id), 1),
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

    for screen in kind.page_screens(page) {
        publish_image_discovery(client, &prefix, device_id, &device, screen).await;
    }

    for id in kind.page_buttons(page) {
        let control = Control::Button(id);
        publish_multi_click_discovery(client, &prefix, device_id, kind, &device, control).await;
    }
}

/// Publishes the switch that turns a button's or knob's multi-click mode on and off. Retained like
/// the images, so it keeps counting its presses after either side restarts.
async fn publish_multi_click_discovery(
    client: &AsyncClient,
    prefix: &str,
    device_id: &str,
    kind: Kind,
    device: &serde_json::Value,
    control: Control,
) {
    let object_id = format!("{}_multi_click", control_object_id(control));
    let payload = json!({
        "name": format!("{} multi-click", kind.control_label(control)),
        "unique_id": format!("{TOPIC_ROOT}_{device_id}_{object_id}"),
        "command_topic": multi_click_command_topic(device_id, control),
        "state_topic": multi_click_topic(device_id, control),
        "payload_on": PAYLOAD_ON,
        "payload_off": PAYLOAD_OFF,
        "retain": true,
        "entity_category": "config",
        "icon": "mdi:gesture-double-tap",
        "device": device,
    });

    publish_discovery_config(client, prefix, "switch", device_id, &object_id, &payload).await;
}

/// Takes back everything `publish_page_discovery` and `publish_multi_click_state` published for
/// the buttons and screens on one page, once the device no longer has that page
pub async fn remove_page_discovery(client: &AsyncClient, device_id: &str, kind: Kind, page: u16) {
    let prefix = discovery_prefix();

    for id in kind.page_buttons(page) {
        let triggers = std::iter::once(format!("button_{id}")).chain(
            MULTI_PRESS_TYPES
                .iter()
                .map(|&(presses, _)| press_payload(Control::Button(id), presses)),
        );

        for object_id in triggers {
            remove_discovery_config(client, &prefix, "device_automation", device_id, &object_id)
                .await;
        }

        let object_id = format!("button_{id}_multi_click");
        remove_discovery_config(client, &prefix, "switch", device_id, &object_id).await;
    }

    for screen in kind.page_screens(page) {
        remove_discovery_config(client, &prefix, "text", device_id, &image_object_id(screen)).await;
    }
}

/// Publishes the text entity that sets the image one screen shows
async fn publish_image_discovery(
    client: &AsyncClient,
    prefix: &str,
    device_id: &str,
    device: &serde_json::Value,
    screen: Screen,
) {
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

    publish_discovery_config(client, prefix, "text", device_id, &object_id, &payload).await;
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

/// Takes back a discovery config, which Home Assistant reads as the entity or trigger being gone.
/// Harmless for one that was never published.
async fn remove_discovery_config(
    client: &AsyncClient,
    prefix: &str,
    component: &str,
    device_id: &str,
    object_id: &str,
) {
    let topic = format!("{prefix}/{component}/{device_id}/{object_id}/config");
    client
        .publish(topic, QoS::AtLeastOnce, true, "")
        .await
        .unwrap_or_else(|_| println!("Failed to remove discovery config for {object_id}"));
}

/// Asks for the commands addressed to one device: how many pages of buttons it has and which one
/// it shows, the images for its screens, which of its buttons and knobs count their presses, the state of its
/// screen timeout, the brightness its screens are lit at and whether its knobs turn the pages.
///
/// This has to be repeated on every new connection, because an MQTT session starts with no
/// subscriptions. The broker replays the retained commands each time, which is what restores the
/// screens and the settings after this app restarts or the connection drops.
pub async fn subscribe_commands(client: &AsyncClient, device_id: &str) {
    // Brokers replay the retained messages for each subscription as they take it on, so asking for
    // the page count first is what has it arrive before the page, which is only accepted when that
    // page exists.
    //
    // One filter covers every screen: the image commands only differ in the button/lcd path. And
    // one covers every button's and knob's multi-click mode, which differ the same way.
    let filters = [
        page_count_command_topic(device_id),
        page_command_topic(device_id),
        format!("{TOPIC_ROOT}/{device_id}/+/+/image/set"),
        format!("{TOPIC_ROOT}/{device_id}/+/+/multi_click/set"),
        timeout_command_topic(device_id),
        brightness_command_topic(device_id),
        paging_command_topic(device_id),
    ];

    for filter in filters {
        if let Err(e) = client.subscribe(&filter, QoS::AtLeastOnce).await {
            println!("[{device_id}] Failed to subscribe to {filter}: {e}");
        }
    }
}

/// Picks out what an incoming message asks one device to do. Messages for another device, and
/// ones on a topic that isn't a command, are not ours to handle.
pub fn command<'a>(publish: &'a Publish, device_id: &str) -> Option<Command<'a>> {
    parse_command(&publish.topic, &publish.payload, device_id)
}

fn parse_command<'a>(topic: &str, payload: &'a [u8], device_id: &str) -> Option<Command<'a>> {
    if let Some(screen) = parse_image_command(topic, device_id) {
        return Some(Command::Image(screen, payload));
    }

    if let Some(control) = parse_multi_click_command(topic, device_id) {
        return parse_switch(payload, device_id, "multi-click")
            .map(|enabled| Command::MultiClick(control, enabled));
    }

    if topic == timeout_command_topic(device_id) {
        return parse_switch(payload, device_id, "screen timeout").map(Command::Timeout);
    }

    if topic == brightness_command_topic(device_id) {
        return parse_brightness(payload, device_id).map(Command::Brightness);
    }

    if topic == page_command_topic(device_id) {
        return parse_number(payload, device_id, "page", 0..=MAX_PAGES - 1).map(Command::Page);
    }

    if topic == page_count_command_topic(device_id) {
        return parse_number(payload, device_id, "page count", 1..=MAX_PAGES)
            .map(Command::PageCount);
    }

    if topic == paging_command_topic(device_id) {
        return parse_switch(payload, device_id, "paging mode").map(Command::Paging);
    }

    None
}

/// Reads an on/off payload for the setting named by `what`, accepting the spellings Home Assistant
/// and the command line both use. An empty payload is a retained command being cleared rather than
/// a setting, so it says nothing about what the setting should be.
fn parse_switch(payload: &[u8], device_id: &str, what: &str) -> Option<bool> {
    let Ok(text) = str::from_utf8(payload) else {
        println!("[{device_id}] Ignoring a {what} command that isn't text");

        return None;
    };

    let command = text.trim();

    match command.to_ascii_lowercase().as_str() {
        "on" | "true" | "1" => Some(true),
        "off" | "false" | "0" => Some(false),
        "" => None,
        _ => {
            println!(
                "[{device_id}] Ignoring {what} command '{}', expected {PAYLOAD_ON} or {PAYLOAD_OFF}",
                Shortened(command)
            );

            None
        }
    }
}

/// Reads a brightness percentage. A level the screens can't be lit at is reported rather than
/// quietly clamped, so a payload in the wrong units doesn't silently darken the device.
fn parse_brightness(payload: &[u8], device_id: &str) -> Option<u8> {
    parse_number(
        payload,
        device_id,
        "screen brightness",
        0..=u16::from(MAX_BRIGHTNESS),
    )
    .map(|percent| percent as u8)
}

/// Reads a whole number for the setting named by `what`. Home Assistant's number entity sends whole
/// numbers, but a template can just as easily produce `40.0`, so both are read. An empty payload is
/// a retained command being cleared rather than a setting, so it says nothing about what the setting
/// should be, and a number outside `range` is reported rather than clamped to it.
fn parse_number(
    payload: &[u8],
    device_id: &str,
    what: &str,
    range: RangeInclusive<u16>,
) -> Option<u16> {
    let Ok(text) = str::from_utf8(payload) else {
        println!("[{device_id}] Ignoring a {what} command that isn't text");

        return None;
    };

    let command = text.trim();

    if command.is_empty() {
        return None;
    }

    let number = command
        .parse::<f64>()
        .ok()
        .map(f64::round)
        .filter(|number| (f64::from(*range.start())..=f64::from(*range.end())).contains(number));

    let Some(number) = number else {
        println!(
            "[{device_id}] Ignoring {what} command '{}', expected a number from {} to {}",
            Shortened(command),
            range.start(),
            range.end()
        );

        return None;
    };

    Some(number as u16)
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

/// Picks out which button or knob a multi-click command is for
fn parse_multi_click_command(topic: &str, device_id: &str) -> Option<Control> {
    let rest = topic
        .strip_prefix(TOPIC_ROOT)?
        .strip_prefix('/')?
        .strip_prefix(device_id)?
        .strip_prefix('/')?
        .strip_suffix("/multi_click/set")?;

    match rest.split_once('/')? {
        ("button", id) => id.parse().ok().map(Control::Button),
        ("knob", id) => id.parse().ok().map(Control::Knob),
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

/// Reports whether the screens dim on their own, so the Home Assistant switch matches the device.
/// Retained, like the image states, so it is still right for a Home Assistant that restarts while
/// this app keeps running.
pub async fn publish_timeout_state(client: &AsyncClient, device_id: &str, enabled: bool) {
    let payload = if enabled { PAYLOAD_ON } else { PAYLOAD_OFF };

    client
        .publish(timeout_topic(device_id), QoS::AtLeastOnce, true, payload)
        .await
        .unwrap_or_else(|_| println!("[{device_id}] Failed to publish the screen timeout state"));
}

/// Reports how bright the screens are lit, so the Home Assistant number matches the device.
/// Retained, like the image and timeout states, so it is still right for a Home Assistant that
/// restarts while this app keeps running.
pub async fn publish_brightness_state(client: &AsyncClient, device_id: &str, percent: u8) {
    client
        .publish(
            brightness_topic(device_id),
            QoS::AtLeastOnce,
            true,
            percent.to_string(),
        )
        .await
        .unwrap_or_else(|_| {
            println!("[{device_id}] Failed to publish the screen brightness state")
        });
}

/// Reports which page of buttons a device is showing, so the Home Assistant number matches the
/// device. Retained, like the other settings.
pub async fn publish_page_state(client: &AsyncClient, device_id: &str, page: u16) {
    client
        .publish(
            page_topic(device_id),
            QoS::AtLeastOnce,
            true,
            page.to_string(),
        )
        .await
        .unwrap_or_else(|_| println!("[{device_id}] Failed to publish the page state"));
}

/// Asks for the page a knob turned the device to, the way Home Assistant would. Retained, as Home
/// Assistant's own command is, which is the point: the broker hands the retained command back on
/// every reconnect, and it has to be the page the device is on rather than the last one Home
/// Assistant asked for, or the device would turn back to that one.
pub async fn publish_page_command(client: &AsyncClient, device_id: &str, page: u16) {
    client
        .publish(
            page_command_topic(device_id),
            QoS::AtLeastOnce,
            true,
            page.to_string(),
        )
        .await
        .unwrap_or_else(|_| println!("[{device_id}] Failed to publish the page command"));
}

/// Reports whether the knobs turn the pages, so the Home Assistant switch matches the device.
/// Retained, like the other settings.
pub async fn publish_paging_state(client: &AsyncClient, device_id: &str, enabled: bool) {
    let payload = if enabled { PAYLOAD_ON } else { PAYLOAD_OFF };

    client
        .publish(paging_topic(device_id), QoS::AtLeastOnce, true, payload)
        .await
        .unwrap_or_else(|_| println!("[{device_id}] Failed to publish the paging mode state"));
}

/// Reports how many pages of buttons a device has, so the Home Assistant number matches the device.
/// Retained, like the other settings.
pub async fn publish_page_count_state(client: &AsyncClient, device_id: &str, pages: u16) {
    client
        .publish(
            page_count_topic(device_id),
            QoS::AtLeastOnce,
            true,
            pages.to_string(),
        )
        .await
        .unwrap_or_else(|_| println!("[{device_id}] Failed to publish the page count state"));
}

/// Reports whether each of some of a device's buttons and knobs counts its presses, so the Home
/// Assistant switches match the device. Retained, like the other settings.
///
/// Also publishes the triggers for pressing one several times in a row for the ones that count
/// their presses, and takes them back for the ones that don't, so a device only offers the triggers
/// that can actually fire.
pub async fn publish_multi_click_states(
    client: &AsyncClient,
    device_id: &str,
    kind: Kind,
    controls: &[Control],
    enabled: &HashSet<Control>,
) {
    for &control in controls {
        let counting = enabled.contains(&control);
        publish_multi_click_state(client, device_id, kind, control, counting).await;
    }
}

/// Reports whether one button or knob counts its presses, and publishes or takes back its triggers
/// for being pressed several times in a row to match. See `publish_multi_click_states`.
pub async fn publish_multi_click_state(
    client: &AsyncClient,
    device_id: &str,
    kind: Kind,
    control: Control,
    enabled: bool,
) {
    let state = if enabled { PAYLOAD_ON } else { PAYLOAD_OFF };

    client
        .publish(
            multi_click_topic(device_id, control),
            QoS::AtLeastOnce,
            true,
            state,
        )
        .await
        .unwrap_or_else(|_| {
            println!(
                "[{device_id}] Failed to publish the multi-click state of {}",
                kind.control_label(control)
            )
        });

    let prefix = discovery_prefix();
    let topic = trigger_topic(device_id);
    let device = device_info(device_id, kind);

    for (presses, trigger_type) in MULTI_PRESS_TYPES {
        // The payload is already unique within the device, so it doubles as the object id
        let object_id = press_payload(control, presses);

        if !enabled {
            remove_discovery_config(client, &prefix, "device_automation", device_id, &object_id)
                .await;

            continue;
        }

        let payload = json!({
            "automation_type": "trigger",
            "platform": "device_automation",
            "topic": topic,
            "type": trigger_type,
            "subtype": kind.control_label(control),
            "payload": press_payload(control, presses),
            "device": device,
        });

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

/// Reports a button or knob being pressed this many times in a row, which is always once for one
/// that doesn't count its presses
pub async fn handle_press(client: &AsyncClient, device_id: &str, control: Control, presses: u32) {
    publish_trigger(client, device_id, &press_payload(control, presses)).await;
}

pub async fn handle_knob(client: &AsyncClient, device_id: &str, i: u8, value: i8) {
    let suffix = if value > 0 { "right" } else { "left" };
    publish_trigger(client, device_id, &format!("knob_{i}_{suffix}")).await;
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

    /// Every screen of the N1 on its first two pages
    fn some_screens() -> Vec<Screen> {
        (0..2)
            .flat_map(|page| Kind::VsdInsideN1.page_screens(page))
            .collect()
    }

    #[test]
    fn a_command_topic_names_the_screen_it_addresses() {
        for screen in some_screens() {
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
        for screen in some_screens() {
            let topic = image_command_topic(SERIAL, screen);
            let levels: Vec<_> = topic.split('/').collect();

            assert_eq!(levels.len(), 6, "{topic} has an unexpected shape");
            assert_eq!(levels[0], TOPIC_ROOT);
            assert_eq!(levels[1], SERIAL);
            assert_eq!(&levels[4..], ["image", "set"]);
        }
    }

    #[test]
    fn the_timeout_command_is_not_mistaken_for_an_image() {
        let topic = timeout_command_topic(SERIAL);

        // Both subscriptions feed the same parser, so the image filter's `+/+` must not swallow
        // the timeout topic and vice versa
        assert_eq!(parse_image_command(&topic, SERIAL), None);
        assert_eq!(
            parse_command(&topic, b"OFF", SERIAL),
            Some(Command::Timeout(false))
        );

        let image = image_command_topic(SERIAL, Screen::Button(0));
        assert_eq!(
            parse_command(&image, b"light.png", SERIAL),
            Some(Command::Image(Screen::Button(0), b"light.png"))
        );
    }

    #[test]
    fn the_timeout_takes_the_spellings_home_assistant_and_a_shell_send() {
        for payload in ["ON", "on", "true", "1", " ON\n"] {
            assert_eq!(
                parse_switch(payload.as_bytes(), SERIAL, "screen timeout"),
                Some(true),
                "'{payload}' should have switched the timeout on"
            );
        }

        for payload in ["OFF", "off", "false", "0", " OFF\n"] {
            assert_eq!(
                parse_switch(payload.as_bytes(), SERIAL, "screen timeout"),
                Some(false),
                "'{payload}' should have switched the timeout off"
            );
        }
    }

    #[test]
    fn a_timeout_payload_that_says_nothing_leaves_the_setting_alone() {
        for payload in [
            // A retained command being cleared, which is not a request to stop dimming
            &b""[..],
            &b"  "[..],
            &b"yes"[..],
            // Not text at all, such as an image sent to the wrong topic
            &[0xff, 0xfe][..],
        ] {
            assert_eq!(parse_switch(payload, SERIAL, "screen timeout"), None);
        }
    }

    #[test]
    fn a_multi_click_command_names_the_button_or_knob_it_addresses() {
        // Including buttons past the first page, and a knob with the same number as a button
        let controls = [0, 8, 16, 17, 271]
            .map(Control::Button)
            .into_iter()
            .chain([0, 2].map(Control::Knob));

        for control in controls {
            let topic = multi_click_command_topic(SERIAL, control);

            assert_eq!(
                parse_command(&topic, b"ON", SERIAL),
                Some(Command::MultiClick(control, true))
            );
            assert_eq!(
                parse_command(&topic, b"OFF", SERIAL),
                Some(Command::MultiClick(control, false))
            );
        }
    }

    #[test]
    fn the_multi_click_command_is_not_mistaken_for_an_image() {
        // The image filter's `+/+` sits at the same depth, so the two must not swallow each other
        for control in [Control::Button(0), Control::Knob(0)] {
            let topic = multi_click_command_topic(SERIAL, control);
            assert_eq!(parse_image_command(&topic, SERIAL), None);
        }

        let image = image_command_topic(SERIAL, Screen::Button(0));
        assert_eq!(parse_multi_click_command(&image, SERIAL), None);
    }

    #[test]
    fn topics_that_are_not_multi_click_commands_are_ignored() {
        for topic in [
            // The state it reports, rather than the command
            &multi_click_topic(SERIAL, Control::Button(0)),
            // Another device's button
            &multi_click_command_topic("CL87654321", Control::Button(0)),
            // Only buttons and knobs count presses
            &format!("{TOPIC_ROOT}/{SERIAL}/lcd/0/multi_click/set"),
            // An id that isn't a number, or doesn't fit one
            &format!("{TOPIC_ROOT}/{SERIAL}/button/left/multi_click/set"),
            &format!("{TOPIC_ROOT}/{SERIAL}/button/70000/multi_click/set"),
            &format!("{TOPIC_ROOT}/{SERIAL}/knob/256/multi_click/set"),
            // Missing the id altogether
            &format!("{TOPIC_ROOT}/{SERIAL}/knob/multi_click/set"),
            // A serial that merely starts with ours
            &format!("{TOPIC_ROOT}/{SERIAL}9/button/0/multi_click/set"),
        ] {
            assert_eq!(
                parse_multi_click_command(topic, SERIAL),
                None,
                "{topic} should not have been treated as a multi-click command"
            );
        }
    }

    #[test]
    fn a_single_press_keeps_its_name_and_more_are_numbered() {
        assert_eq!(press_payload(Control::Button(3), 1), "button_3_press");
        assert_eq!(press_payload(Control::Button(3), 2), "button_3_press_2");
        assert_eq!(press_payload(Control::Button(16), 7), "button_16_press_7");
        assert_eq!(press_payload(Control::Knob(0), 1), "knob_0_press");
        assert_eq!(press_payload(Control::Knob(2), 3), "knob_2_press_3");
    }

    #[test]
    fn a_timeout_command_for_another_device_is_not_ours() {
        let topic = timeout_command_topic("CL87654321");

        assert_eq!(parse_command(&topic, b"OFF", SERIAL), None);
    }

    #[test]
    fn the_brightness_command_is_not_mistaken_for_an_image_or_the_timeout() {
        let topic = brightness_command_topic(SERIAL);

        assert_eq!(parse_image_command(&topic, SERIAL), None);
        assert_eq!(
            parse_command(&topic, b"60", SERIAL),
            Some(Command::Brightness(60))
        );

        // And the timeout topic, which sits at the same depth, is still the timeout's
        assert_eq!(
            parse_command(&timeout_command_topic(SERIAL), b"OFF", SERIAL),
            Some(Command::Timeout(false))
        );
    }

    #[test]
    fn the_brightness_takes_the_spellings_home_assistant_and_a_shell_send() {
        // Home Assistant's number entity sends whole numbers, a template can produce a decimal,
        // and a shell send may bring a trailing newline along
        for (payload, expected) in [
            ("0", 0),
            ("40", 40),
            ("100", 100),
            ("40.0", 40),
            ("39.6", 40),
            (" 40\n", 40),
        ] {
            assert_eq!(
                parse_brightness(payload.as_bytes(), SERIAL),
                Some(expected),
                "'{payload}' should have set the brightness to {expected}"
            );
        }
    }

    #[test]
    fn a_brightness_payload_that_says_nothing_leaves_the_setting_alone() {
        for payload in [
            // A retained command being cleared, which is not a request to go dark
            &b""[..],
            &b"  "[..],
            // Outside the range the screens can be lit at, rather than clamped to it
            &b"101"[..],
            &b"-1"[..],
            &b"inf"[..],
            &b"NaN"[..],
            // Not a number at all
            &b"bright"[..],
            &b"40%"[..],
            // Not text at all, such as an image sent to the wrong topic
            &[0xff, 0xfe][..],
        ] {
            assert_eq!(parse_brightness(payload, SERIAL), None);
        }
    }

    #[test]
    fn page_commands_are_told_apart_from_each_other_and_the_rest() {
        assert_eq!(
            parse_command(&page_command_topic(SERIAL), b"2", SERIAL),
            Some(Command::Page(2))
        );
        assert_eq!(
            parse_command(&page_count_command_topic(SERIAL), b"3", SERIAL),
            Some(Command::PageCount(3))
        );

        for topic in [page_command_topic(SERIAL), page_count_command_topic(SERIAL)] {
            assert_eq!(parse_image_command(&topic, SERIAL), None);
            assert_eq!(parse_multi_click_command(&topic, SERIAL), None);
        }

        // Nor are the states they report, or another device's commands
        assert_eq!(parse_command(&page_topic(SERIAL), b"2", SERIAL), None);
        assert_eq!(parse_command(&page_count_topic(SERIAL), b"3", SERIAL), None);
        assert_eq!(
            parse_command(&page_command_topic("CL87654321"), b"2", SERIAL),
            None
        );
    }

    #[test]
    fn pages_are_counted_from_zero_and_there_is_at_least_one() {
        let page = page_command_topic(SERIAL);
        let count = page_count_command_topic(SERIAL);

        // A template's decimal is read like the brightness's
        assert_eq!(
            parse_command(&page, b" 1.0\n", SERIAL),
            Some(Command::Page(1))
        );
        assert_eq!(parse_command(&page, b"0", SERIAL), Some(Command::Page(0)));
        assert_eq!(
            parse_command(&count, MAX_PAGES.to_string().as_bytes(), SERIAL),
            Some(Command::PageCount(MAX_PAGES))
        );

        for payload in [
            MAX_PAGES.to_string(),
            "-1".to_string(),
            "".to_string(),
            "next".to_string(),
        ] {
            assert_eq!(parse_command(&page, payload.as_bytes(), SERIAL), None);
        }

        for payload in ["0", "", "-1", &(MAX_PAGES + 1).to_string()] {
            assert_eq!(parse_command(&count, payload.as_bytes(), SERIAL), None);
        }
    }

    #[test]
    fn the_paging_mode_command_is_told_apart_from_the_rest() {
        let topic = paging_command_topic(SERIAL);

        assert_eq!(
            parse_command(&topic, b"ON", SERIAL),
            Some(Command::Paging(true))
        );
        assert_eq!(
            parse_command(&topic, b"off", SERIAL),
            Some(Command::Paging(false))
        );
        assert_eq!(parse_command(&topic, b"", SERIAL), None);

        // The page command sits right next to it, and is still the page's
        assert_eq!(parse_image_command(&topic, SERIAL), None);
        assert_eq!(
            parse_command(&page_command_topic(SERIAL), b"1", SERIAL),
            Some(Command::Page(1))
        );

        // Nor is the state it reports, or another device's command
        assert_eq!(parse_command(&paging_topic(SERIAL), b"ON", SERIAL), None);
        assert_eq!(
            parse_command(&paging_command_topic("CL87654321"), b"ON", SERIAL),
            None
        );
    }

    #[test]
    fn a_brightness_command_for_another_device_is_not_ours() {
        let topic = brightness_command_topic("CL87654321");

        assert_eq!(parse_command(&topic, b"60", SERIAL), None);
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

        // And the same for the settings, where reporting the state back as a command would be a
        // loop rather than just a wrong screen
        assert_ne!(timeout_topic(SERIAL), timeout_command_topic(SERIAL));
        assert_eq!(parse_command(&timeout_topic(SERIAL), b"OFF", SERIAL), None);

        assert_ne!(brightness_topic(SERIAL), brightness_command_topic(SERIAL));
        assert_eq!(
            parse_command(&brightness_topic(SERIAL), b"60", SERIAL),
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
            &format!("{TOPIC_ROOT}/{SERIAL}/button/70000/image/set"),
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
