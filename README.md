# Headless Stream Dock HA controller

This project allows you to use "Stream dock" such as Mirabox N3 connected to (headless) Linux as a Home Assistant input device. Each button and knob is registered with Home Assistant over MQTT discovery as a device trigger, so you build the actual automations (what a button press or knob turn should do) in Home Assistant itself, rather than hardcoding them here. The project uses [Mirajazz library](https://github.com/4ndv/mirajazz/) and is partially derived from [OpenDeck Ajazz AKP03 / Mirabox N3 plugin](https://github.com/4ndv/opendeck-akp03).

## Before running the app

To detect the device, you have to set some udev rules. Download the [udev rules](https://github.com/4ndv/opendeck-akp03/blob/main/40-opendeck-akp03.rules) and install them by copying into `/etc/udev/rules.d/` and running `sudo udevadm control --reload-rules`. Unplug and plug again the device after this.

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
```

All images referenced in the config should be placed in the `images/` directory

## Setting up automations in Home Assistant

Once the app is running and connected to your MQTT broker, it publishes discovery data and a new device named "Stream Dock (...)" appears under Settings -> Devices & Services -> MQTT. From there you can build one "Device" automation per button/knob trigger (Settings -> Automations -> Add Automation -> When -> Device -> select the Stream Dock device -> pick a trigger such as "Button 1 pressed" or "Knob 0 rotated left/right").

Creating a separate automation per button gets unwieldy quickly, though, so a [blueprint](blueprints/streamdock_actions.yaml) is included that maps every button and knob for one device in a single automation:

1. Settings -> Automations -> Blueprints -> Import Blueprint, and point it at the raw contents of [blueprints/streamdock_actions.yaml](blueprints/streamdock_actions.yaml) (or copy the file into your `config/blueprints/automation/<name>/` folder and reload blueprints).
2. Create a new automation from the imported blueprint.
3. Fill in the trigger topic (`streamdock/<serial>/trigger`, visible in the device's MQTT discovery config) and an action for each button/knob you want to use; unused ones can stay empty.

## Compatibility

This program is only tested with Mirabox N3, but the underlying library supports also similar devices such as Ajazz AKP03. To get parameters for your device, you can examine the `dmesg` output and you should see something like:

```
New USB device found, idVendor=6603, idProduct=1003, bcdDevice= 0.02
```

Which means `vendor_id = 0x6603` and `product_id = 0x1003`.

## Features

- Register buttons and knobs as Home Assistant MQTT device triggers, so actions are defined via HA automations
- Includes a [blueprint](blueprints/streamdock_actions.yaml) to map all buttons/knobs in a single automation
- Set custom pictures for buttons with screens
- Configure timeout for screens
- Configure screen brightness
