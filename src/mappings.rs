use std::time::Duration;

use mirajazz::{
    device::DeviceQuery,
    error::MirajazzError,
    types::{DeviceInput, ImageFormat, ImageMirroring, ImageMode, ImageRotation},
};

use crate::inputs;

pub const MIRABOX_VID: u16 = 0x6603;
pub const N3_PID: u16 = 0x1003;

pub const VSDINSIDE_VID: u16 = 0x5548;
pub const N1_PID: u16 = 0x1002;

/// All devices are behind usage page 65440, usage id 1
pub const QUERIES: &[DeviceQuery] = &[
    DeviceQuery::new(65440, 1, MIRABOX_VID, N3_PID),
    DeviceQuery::new(65440, 1, VSDINSIDE_VID, N1_PID),
];

/// Supported models. Every device-specific parameter lives here, so the rest of the
/// program stays model-agnostic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// Mirabox Stream Dock N3 (and compatible devices such as the Ajazz AKP03)
    MiraboxN3,
    /// VSD Inside N1 (and compatible devices such as the Mirabox N1)
    VsdInsideN1,
}

impl Kind {
    /// Matches a device's VID+PID pair to the correct kind
    pub fn from_vid_pid(vid: u16, pid: u16) -> Option<Self> {
        match (vid, pid) {
            (MIRABOX_VID, N3_PID) => Some(Kind::MiraboxN3),
            (VSDINSIDE_VID, N1_PID) => Some(Kind::VsdInsideN1),
            _ => None,
        }
    }

    /// Names reported by the USB stack are unreliable, so use our own
    pub fn manufacturer(&self) -> &'static str {
        match self {
            Kind::MiraboxN3 => "Mirabox",
            Kind::VsdInsideN1 => "VSD Inside",
        }
    }

    pub fn model(&self) -> &'static str {
        match self {
            Kind::MiraboxN3 => "Stream Dock N3",
            Kind::VsdInsideN1 => "N1",
        }
    }

    pub fn protocol_version(&self) -> usize {
        match self {
            Kind::MiraboxN3 | Kind::VsdInsideN1 => 3,
        }
    }

    /// Total number of buttons, including the ones without a screen
    pub fn key_count(&self) -> usize {
        match self {
            Kind::MiraboxN3 => 9,
            Kind::VsdInsideN1 => 17,
        }
    }

    pub fn encoder_count(&self) -> usize {
        match self {
            Kind::MiraboxN3 => 3,
            Kind::VsdInsideN1 => 1,
        }
    }

    /// Buttons `0..screen_key_count` have a screen and can show an image, the remaining
    /// ones are input-only. For both models the button id doubles as the hardware image id.
    pub fn screen_key_count(&self) -> usize {
        match self {
            Kind::MiraboxN3 => 6,
            Kind::VsdInsideN1 => 15,
        }
    }

    pub fn image_format(&self) -> ImageFormat {
        match self {
            Kind::MiraboxN3 => ImageFormat {
                mode: ImageMode::JPEG,
                size: (60, 60),
                rotation: ImageRotation::Rot90,
                mirror: ImageMirroring::None,
            },
            Kind::VsdInsideN1 => ImageFormat {
                mode: ImageMode::JPEG,
                size: (96, 96),
                rotation: ImageRotation::Rot0,
                mirror: ImageMirroring::None,
            },
        }
    }

    /// Number of segments on the secondary LCD strip. The strip is image-only, it reports
    /// no input, and models without one return 0.
    pub fn lcd_segment_count(&self) -> usize {
        match self {
            Kind::MiraboxN3 => 0,
            Kind::VsdInsideN1 => 3,
        }
    }

    /// The LCD strip segments continue the hardware image ids right after the keys
    pub fn lcd_hw_key(&self, segment: u8) -> Option<u8> {
        if (segment as usize) < self.lcd_segment_count() {
            Some(self.screen_key_count() as u8 + segment)
        } else {
            None
        }
    }

    pub fn lcd_image_format(&self) -> ImageFormat {
        match self {
            // Never used, the N3 has no LCD strip
            Kind::MiraboxN3 => self.image_format(),
            Kind::VsdInsideN1 => ImageFormat {
                mode: ImageMode::JPEG,
                // N1 second screen segments are 64x64 each
                size: (64, 64),
                rotation: ImageRotation::Rot0,
                mirror: ImageMirroring::None,
            },
        }
    }

    /// Some devices ignore every other command until they are put into the right mode
    pub fn startup_mode(&self) -> Option<u8> {
        match self {
            Kind::MiraboxN3 => None,
            Kind::VsdInsideN1 => Some(3),
        }
    }

    /// Some devices drop the connection when idle unless they are pinged periodically
    pub fn keepalive_interval(&self) -> Option<Duration> {
        match self {
            Kind::MiraboxN3 => None,
            Kind::VsdInsideN1 => Some(Duration::from_secs(10)),
        }
    }

    /// Function that turns this model's raw reports into device inputs
    pub fn process_input(&self) -> fn(u8, u8) -> Result<DeviceInput, MirajazzError> {
        match self {
            Kind::MiraboxN3 => inputs::process_input_n3,
            Kind::VsdInsideN1 => inputs::process_input_n1,
        }
    }

    /// Human readable button name, used as the subtype of the Home Assistant trigger
    pub fn button_label(&self, id: u8) -> String {
        match (self, id) {
            (Kind::VsdInsideN1, 15) => "Top button left".to_string(),
            (Kind::VsdInsideN1, 16) => "Top button right".to_string(),
            _ => format!("Button {id}"),
        }
    }

    pub fn knob_label(&self, id: u8) -> String {
        format!("Knob {id}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_devices_are_recognized() {
        assert_eq!(
            Kind::from_vid_pid(MIRABOX_VID, N3_PID),
            Some(Kind::MiraboxN3)
        );
        assert_eq!(
            Kind::from_vid_pid(VSDINSIDE_VID, N1_PID),
            Some(Kind::VsdInsideN1)
        );
        // Not a mix-and-match of the two
        assert_eq!(Kind::from_vid_pid(MIRABOX_VID, N1_PID), None);
        assert_eq!(Kind::from_vid_pid(0x1234, 0x5678), None);
    }

    #[test]
    fn every_device_has_more_keys_than_screens() {
        for kind in [Kind::MiraboxN3, Kind::VsdInsideN1] {
            assert!(kind.screen_key_count() <= kind.key_count());
        }
    }

    #[test]
    fn lcd_segments_follow_the_keys_in_hardware_ids() {
        // The N1's three LCD segments live right after its 15 key screens
        assert_eq!(Kind::VsdInsideN1.lcd_hw_key(0), Some(15));
        assert_eq!(Kind::VsdInsideN1.lcd_hw_key(1), Some(16));
        assert_eq!(Kind::VsdInsideN1.lcd_hw_key(2), Some(17));
        assert_eq!(Kind::VsdInsideN1.lcd_hw_key(3), None);

        // The N3 has no LCD strip at all
        assert_eq!(Kind::MiraboxN3.lcd_segment_count(), 0);
        assert_eq!(Kind::MiraboxN3.lcd_hw_key(0), None);
    }
}
