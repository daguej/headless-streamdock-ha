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

`config.toml` contains general configuration (brightness, timeout) and optional button icons. Buttons and knobs work as triggers even when not listed here; the only thing a `[[buttons]]` entry adds is an image set on the device's screen. Knobs need no configuration at all.

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
- Configure timeout for screens
- Configure screen brightness
