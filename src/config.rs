use serde::Deserialize;
use std::collections::HashSet;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub brightness: u8,
    pub timeout: u64,
    #[serde(default)]
    pub buttons: Vec<ImageConfig>,
    /// Segments of the secondary LCD strip, on devices that have one
    #[serde(default)]
    pub lcd: Vec<ImageConfig>,
    /// Per-device overrides of the settings above, matched by serial number
    #[serde(default)]
    pub devices: Vec<DeviceOverride>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ImageConfig {
    pub id: u8,
    pub icon: String,
}

/// Settings for one particular device. Anything left out falls back to the top level of the
/// config file, so a single device only needs an entry here if it should differ from the rest.
#[derive(Debug, Deserialize)]
pub struct DeviceOverride {
    pub serial: String,
    pub brightness: Option<u8>,
    pub timeout: Option<u64>,
    pub buttons: Option<Vec<ImageConfig>>,
    pub lcd: Option<Vec<ImageConfig>>,
}

/// The settings that apply to a single connected device, with the overrides already resolved
/// against the defaults
#[derive(Debug, Clone)]
pub struct DeviceConfig {
    pub brightness: u8,
    pub timeout: u64,
    pub buttons: Vec<ImageConfig>,
    pub lcd: Vec<ImageConfig>,
}

impl Config {
    /// Resolves the settings for the device with the given serial number. A `[[devices]]`
    /// entry replaces the icon lists wholesale rather than merging them by id, so the entry
    /// describes everything that device shows.
    pub fn for_device(&self, serial: &str) -> DeviceConfig {
        let over = self.devices.iter().find(|d| d.serial == serial);

        DeviceConfig {
            brightness: over.and_then(|o| o.brightness).unwrap_or(self.brightness),
            timeout: over.and_then(|o| o.timeout).unwrap_or(self.timeout),
            buttons: over
                .and_then(|o| o.buttons.clone())
                .unwrap_or_else(|| self.buttons.clone()),
            lcd: over
                .and_then(|o| o.lcd.clone())
                .unwrap_or_else(|| self.lcd.clone()),
        }
    }

    /// Two entries for the same serial number would silently leave one of them unused
    fn validate(&self) -> Result<(), String> {
        let mut seen = HashSet::new();

        for device in &self.devices {
            if !seen.insert(device.serial.as_str()) {
                return Err(format!(
                    "more than one [[devices]] entry for serial '{}'",
                    device.serial
                ));
            }
        }

        Ok(())
    }
}

pub fn load_config() -> Result<Config, Box<dyn std::error::Error>> {
    let config_text = std::fs::read_to_string("config.toml")?;
    let config: Config = toml::from_str(&config_text)?;
    config.validate()?;
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(text: &str) -> Config {
        let config: Config = toml::from_str(text).expect("config should parse");
        config.validate().expect("config should be valid");
        config
    }

    fn icons(images: &[ImageConfig]) -> Vec<(u8, &str)> {
        images.iter().map(|i| (i.id, i.icon.as_str())).collect()
    }

    const DEFAULTS_ONLY: &str = r#"
        brightness = 40
        timeout = 30

        [[buttons]]
        id = 0
        icon = "tux.png"

        [[lcd]]
        id = 0
        icon = "clock.png"
    "#;

    #[test]
    fn a_config_without_device_sections_applies_to_every_device() {
        let config = parse(DEFAULTS_ONLY);

        for serial in ["AL12345678", "CL87654321"] {
            let resolved = config.for_device(serial);

            assert_eq!(resolved.brightness, 40);
            assert_eq!(resolved.timeout, 30);
            assert_eq!(icons(&resolved.buttons), vec![(0, "tux.png")]);
            assert_eq!(icons(&resolved.lcd), vec![(0, "clock.png")]);
        }
    }

    #[test]
    fn a_device_section_only_overrides_what_it_sets() {
        let config = parse(&format!(
            r#"
            {DEFAULTS_ONLY}

            [[devices]]
            serial = "AL12345678"
            brightness = 80
            "#
        ));

        let overridden = config.for_device("AL12345678");
        assert_eq!(overridden.brightness, 80);
        // Everything the entry left out still comes from the top level
        assert_eq!(overridden.timeout, 30);
        assert_eq!(icons(&overridden.buttons), vec![(0, "tux.png")]);
        assert_eq!(icons(&overridden.lcd), vec![(0, "clock.png")]);

        // And a device without an entry is untouched
        assert_eq!(config.for_device("CL87654321").brightness, 40);
    }

    #[test]
    fn device_icons_replace_the_defaults_instead_of_merging() {
        let config = parse(&format!(
            r#"
            {DEFAULTS_ONLY}

            [[devices]]
            serial = "AL12345678"

            [[devices.buttons]]
            id = 3
            icon = "light.png"
            "#
        ));

        let resolved = config.for_device("AL12345678");
        // Button 0's default icon is gone rather than kept alongside button 3
        assert_eq!(icons(&resolved.buttons), vec![(3, "light.png")]);
        // The LCD list wasn't mentioned, so it still falls back
        assert_eq!(icons(&resolved.lcd), vec![(0, "clock.png")]);
    }

    #[test]
    fn an_empty_icon_list_blanks_the_device() {
        let config = parse(&format!(
            r#"
            {DEFAULTS_ONLY}

            [[devices]]
            serial = "AL12345678"
            buttons = []
            "#
        ));

        assert!(config.for_device("AL12345678").buttons.is_empty());
    }

    #[test]
    fn duplicate_device_entries_are_rejected() {
        let config: Config = toml::from_str(&format!(
            r#"
            {DEFAULTS_ONLY}

            [[devices]]
            serial = "AL12345678"

            [[devices]]
            serial = "AL12345678"
            brightness = 10
            "#
        ))
        .unwrap();

        assert!(config.validate().is_err());
    }
}
