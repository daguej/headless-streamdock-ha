# Headless Stream Dock HA controller

This project allows you to use "Stream dock" devices such as the Mirabox N3 or the VSD Inside N1 connected to (headless) Linux as a Home Assistant input device. Each button and knob is registered with Home Assistant over MQTT discovery as a device trigger, so you build the actual automations (what a button press or knob turn should do) in Home Assistant itself, rather than hardcoding them here. The project uses [Mirajazz library](https://github.com/4ndv/mirajazz/) and is partially derived from [OpenDeck Ajazz AKP03 / Mirabox N3 plugin](https://github.com/4ndv/opendeck-akp03) and [OpenDeck VSD Inside N1 plugin](https://github.com/rattenjunge-samu/opendeck-vsd-n1).

## Supported devices

| Device | USB ID | Buttons | Knobs | Screens |
| --- | --- | --- | --- | --- |
| Mirabox Stream Dock N3 (and compatible, e.g. Ajazz AKP03) | `6603:1003` | 9 (ids 0-8) | 3 (ids 0-2) | buttons 0-5 |
| VSD Inside N1 (and compatible, e.g. Mirabox N1) | `5548:1002` | 17 (ids 0-16) | 1 (id 0) | buttons 0-14, plus a 3-segment LCD strip |

Connected devices are detected by their USB ID, so nothing has to be configured to pick a model. On the N1, buttons 15 and 16 are the two buttons above the LCD strip and have no screen of their own, and the LCD strip itself only shows images, it reports no input.

Several devices can be connected at the same time, in any mix of models. Each one runs independently: it gets its own Home Assistant device, its own MQTT topic and its own section of the config, and a device that errors out or is unplugged doesn't disturb the others. Devices are also picked up and dropped while the app runs, so plugging one in doesn't need a restart.

## Before running the app

To detect the device, you have to set some udev rules. Copy the included [40-headless-streamdock.rules](40-headless-streamdock.rules) into `/etc/udev/rules.d/` and run `sudo udevadm control --reload-rules`. The rules give the device to the `plugdev` group, so make sure the user running this program is a member of it (`sudo usermod -aG plugdev <user>`); the file also contains a `uaccess` variant for desktop use. Unplug and plug the device in again after this.

You also need an MQTT broker connected to Home Assistant's [MQTT integration](https://www.home-assistant.io/integrations/mqtt/). If you don't have one yet, the easiest option is installing the official "Mosquitto broker" add-on from the Home Assistant add-on store, then setting up the MQTT integration to use it (Settings -> Devices & Services -> Add Integration -> MQTT).

## Configuration

The configuration of this app consists of three things: `.env` file (or other env variables), `config.toml` file and `images/` directory for button images.

`.env` file should contain the MQTT broker connection details:

```dotenv
MQTT_HOST="localhost"
MQTT_PORT=1883 # optional, defaults to 1883
MQTT_USERNAME="streamdock" # optional
MQTT_PASSWORD="verysecretpassword" # optional
MQTT_DISCOVERY_PREFIX="homeassistant" # optional, only needed if you changed HA's default discovery prefix
```

`config.toml` contains general configuration (brightness, timeout) and optional button icons. Buttons and knobs work as triggers even when not listed here; the only thing a `[[buttons]]` entry adds is the image a screen starts out with. Once the app is running, Home Assistant can change any screen at any time (see [Controlling the screens from Home Assistant](#controlling-the-screens-from-home-assistant)), so these entries are the fallback the device shows before Home Assistant has said otherwise. Knobs need no configuration at all.

```toml
brightness = 40 # key screen brightness, 0-100
timeout = 30 # timeout in seconds for turning off the key screens

[[buttons]]
id = 0 	# id of the button
icon = "light.png" # icon for button, corresponding file must be in `images/` directory

[[buttons]]
id = 5
icon = "candle.png"

# Only on devices with an LCD strip, such as the N1
[[lcd]]
id = 0 # segment of the LCD strip, left to right
icon = "clock.png"
```

All images referenced in the config should be placed in the `images/` directory. Entries for buttons or LCD segments the connected device doesn't have (for example `id = 8` on a device whose screens stop at button 5) are skipped with a warning, so the same config file can be used with either model. An icon that can't be read leaves that button blank, it doesn't stop the device.

### Giving one device its own settings

Everything above applies to every connected device. When you have more than one plugged in and they shouldn't all look the same, add a `[[devices]]` section keyed by the device's serial number:

```toml
brightness = 40
timeout = 30

[[buttons]]
id = 0
icon = "light.png"

[[devices]]
serial = "AL12345678" # serial number of the device this section applies to
brightness = 80       # this dock is somewhere brighter

[[devices.buttons]]
id = 0
icon = "candle.png"

[[devices.lcd]]
id = 0
icon = "clock.png"
```

The serial number is the one the app prints when it connects (`[AL12345678] Connecting to ...`); it is also the `<serial>` in the MQTT topic and in the Home Assistant device name.

Anything a `[[devices]]` section leaves out falls back to the top level, so a device that only needs a different brightness only has to set `brightness`. The `buttons` and `lcd` lists are the exception: if a section lists any, they *replace* the top-level list for that device rather than being merged into it, so the section describes everything that device shows. Use `buttons = []` to give a device no icons at all.

## Controlling the screens from Home Assistant

Every screen the connected device has gets a text entity named after it ("Button 0 image", "LCD segment 0 image"), listed under Configuration on the device page alongside the [screen timeout switch](#keeping-the-screens-lit). Typing the name of a file from `images/` into one sets that screen, and emptying the field blanks it, so the screens can be changed from a dashboard without touching the config file.

The entity's value is also what the screen is showing right now, and it is published retained, so it stays right across a Home Assistant restart.

### From an automation

The entities are backed by one MQTT topic per screen, which an automation can publish to directly:

| Topic | What it does |
| --- | --- |
| `streamdock/<serial>/button/<id>/image/set` | Sets the image on a button's screen |
| `streamdock/<serial>/lcd/<id>/image/set` | Sets the image on an LCD strip segment |
| `streamdock/<serial>/button/<id>/image` | Reports what that button is showing (published by the app, don't write to it) |

The payload can be any of:

- the name of a file in `images/`, such as `light.png`
- base64 encoded image data, optionally as a `data:image/png;base64,...` URI and optionally wrapped across lines, which is what a Home Assistant template can produce
- raw image bytes, which is what `mosquitto_pub -f` sends
- nothing at all, which blanks the screen

Whichever it is is worked out from the payload itself, so the same topic takes all of them. Any format the [image](https://crates.io/crates/image) crate reads works (PNG, JPEG, GIF, WebP, BMP and more); it is resized to the screen automatically. A message may be up to 512 KiB.

Picking an icon by name is the usual case, and an ordinary automation covers it:

```yaml
triggers:
  - trigger: state
    entity_id: light.kitchen
actions:
  - action: mqtt.publish
    data:
      topic: streamdock/AL12345678/button/0/image/set
      retain: true
      payload: "{{ 'light-on.png' if is_state('light.kitchen', 'on') else 'light-off.png' }}"
```

An image generated elsewhere is published the same way, as base64:

```yaml
  - action: mqtt.publish
    data:
      topic: streamdock/AL12345678/button/1/image/set
      payload: "{{ state_attr('sensor.doorbell_thumbnail', 'image_base64') }}"
```

Or, from anything that can talk to the broker, as the bytes of a file:

```bash
mosquitto_pub -h localhost -t 'streamdock/AL12345678/button/0/image/set' -f new-icon.png -r
```

### Making images stick

Publish with `retain: true` (which the text entities do for you) and the broker hands the image back every time this app reconnects, so the screens come back as they were after a restart of either side, of the broker, or after unplugging the dock. Without it, an image lasts only until the device next reconnects, and the screen then falls back to whatever `config.toml` gives it.

Because a screen showing an image that arrived over MQTT has no file name to report, its entity reads `<image>` instead. Submitting that unchanged does nothing, so the screen isn't cleared by looking at it.

Two smaller things worth knowing: an image sent while the screens have dimmed is drawn but stays dim until the next button press, and a command for a screen the connected device doesn't have is logged and ignored, the same way the config entries are.

## Keeping the screens lit

`timeout` in `config.toml` decides how long a device may sit untouched before its screens dim. For a dock that should stay lit regardless — one showing a dashboard rather than waiting to be pressed — each device also gets a "Screen timeout" switch, listed under Configuration on the device page next to the image entities.

Turning it off keeps the screens lit however long the device is left alone, and brings screens that have already dimmed straight back up. Turning it back on starts the idle period over, so the screens get another `timeout` seconds before they dim rather than going dark the moment the switch flips.

### Switching it from an automation

The switch is backed by one MQTT topic per device, like the screens:

| Topic | What it does |
| --- | --- |
| `streamdock/<serial>/timeout/set` | `ON` lets the screens dim after `timeout` seconds, `OFF` keeps them lit |
| `streamdock/<serial>/timeout` | Reports which of the two it currently is (published by the app, don't write to it) |

```yaml
triggers:
  - trigger: state
    entity_id: binary_sensor.kitchen_presence
    to: "on"
actions:
  - action: mqtt.publish
    data:
      topic: streamdock/AL12345678/timeout/set
      retain: true
      payload: "OFF"
```

`ON` and `OFF` are what Home Assistant sends, and `true`/`false` and `1`/`0` are accepted too, in any case. Publish with `retain: true` (which the switch does for you) and the setting survives a restart of either side, exactly as the images do; without it the screens dim again as soon as the device next reconnects. A payload that is neither on nor off is logged and ignored, leaving the setting as it was.

## Setting up automations in Home Assistant

Once the app is running and connected to your MQTT broker, it publishes discovery data and a new device named "Stream Dock (...)" appears under Settings -> Devices & Services -> MQTT. From there you can build one "Device" automation per button/knob trigger (Settings -> Automations -> Add Automation -> When -> Device -> select the Stream Dock device -> pick a trigger such as "Button 1 pressed" or "Knob 0 rotated left/right").

Creating a separate automation per button gets unwieldy quickly, though, so blueprints are included that map every button and knob for one device in a single automation. Use [blueprints/streamdock_actions.yaml](blueprints/streamdock_actions.yaml) for the N3 and [blueprints/streamdock_n1_actions.yaml](blueprints/streamdock_n1_actions.yaml) for the N1:

1. Settings -> Automations -> Blueprints -> Import Blueprint, and point it at the raw contents of the blueprint for your device (or copy the file into your `config/blueprints/automation/<name>/` folder and reload blueprints).
2. Create a new automation from the imported blueprint.
3. Fill in the trigger topic (`streamdock/<serial>/trigger`, visible in the device's MQTT discovery config) and an action for each button/knob you want to use; unused ones can stay empty.

Because the topic is per device, several connected devices each get their own automation created from the same blueprint, one per serial number.

## Compatibility

Adding another device of the same family usually only takes a new entry in [src/mappings.rs](src/mappings.rs) and, if its report codes differ, a decoder in [src/inputs.rs](src/inputs.rs). To get the USB ID for your device, you can examine the `dmesg` output and you should see something like:

```
New USB device found, idVendor=6603, idProduct=1003, bcdDevice= 0.02
```

Which means `vendor_id = 0x6603` and `product_id = 0x1003`. If the device is recognized but sends button codes this program doesn't know, it prints `Ignoring unknown N1 input: code=0x..` lines you can use to work out the mapping. The [OpenDeck wiki](https://github.com/4ndv/opendeck-akp03/wiki/Adding-support-for-new-devices) has more on this.

## Features

- Register buttons and knobs as Home Assistant MQTT device triggers, so actions are defined via HA automations
- Support for multiple device models, detected automatically by USB ID
- Run several devices at once, each with its own HA device and optionally its own settings
- Pick up devices as they are plugged in and drop them as they are unplugged, without a restart
- Includes [blueprints](blueprints/) to map all buttons/knobs in a single automation
- Set custom pictures for buttons with screens, and for the LCD strip on devices that have one
- Change any screen from Home Assistant while running, either by naming a file or by sending the image itself over MQTT
- Configure timeout for screens, and turn it off from Home Assistant while running to keep a device lit
- Configure screen brightness
