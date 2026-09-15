use std::{ops::Range, time::Duration};

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

/// The most pages of buttons Home Assistant can give a device. Every page is another full set of
/// triggers, image entities and multi-click switches, all of them retained, so this keeps a
/// mistyped page count from flooding the broker and Home Assistant with thousands of them.
pub const MAX_PAGES: u16 = 16;

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

/// Somewhere an image can be shown: the screen in a button, or one segment of the LCD strip.
/// Which of these a device actually has depends on its [`Kind`], so a screen only becomes a
/// hardware image id by way of [`Kind::resolve_screen`].
///
/// Both are identified by their id across every page (see [`Kind::button_id`] and
/// [`Kind::lcd_id`]), so the same physical screen is a different `Screen` on each page.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Screen {
    Button(u16),
    Lcd(u16),
}

/// Something that can be pressed: a button, or a knob pushed in.
///
/// A button is identified by its id across every page (see [`Kind::button_id`]), the same way its
/// screen is. Knobs do the same thing on every page, so a knob is just which knob it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Control {
    Button(u16),
    Knob(u8),
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

    /// Identifies a button across every page: the buttons on each page are numbered on from the
    /// last one of the page before, so on a model with 17 buttons the first one of the second page
    /// is button 17
    pub fn button_id(&self, page: u16, key: u8) -> u16 {
        page * self.key_count() as u16 + u16::from(key)
    }

    /// The page a button is on, and which of the physical buttons it is there. The reverse of
    /// [`Kind::button_id`].
    pub fn locate_button(&self, id: u16) -> (u16, u8) {
        locate(id, self.key_count())
    }

    /// Identifies an LCD strip segment across every page, numbered on from the page before the same
    /// way buttons are, so on a strip of 3 segments the first one of the second page is segment 3
    pub fn lcd_id(&self, page: u16, segment: u8) -> u16 {
        page * self.lcd_segment_count() as u16 + u16::from(segment)
    }

    /// The page an LCD strip segment is on, and which of the physical segments it is there. The
    /// reverse of [`Kind::lcd_id`].
    pub fn locate_lcd(&self, id: u16) -> (u16, u8) {
        locate(id, self.lcd_segment_count())
    }

    /// Every button on one page, screen or not
    pub fn page_buttons(&self, page: u16) -> impl Iterator<Item = u16> {
        let first = self.button_id(page, 0);

        first..first + self.key_count() as u16
    }

    /// Every screen on one page, buttons first and then the LCD strip
    pub fn page_screens(&self, page: u16) -> Vec<Screen> {
        let buttons =
            (0..self.screen_key_count() as u8).map(|key| Screen::Button(self.button_id(page, key)));
        let lcd = (0..self.lcd_segment_count() as u8)
            .map(|segment| Screen::Lcd(self.lcd_id(page, segment)));

        buttons.chain(lcd).collect()
    }

    /// The page a screen is on. Only meaningful for a screen this model has, which
    /// [`Kind::resolve_screen`] tells.
    pub fn screen_page(&self, screen: Screen) -> u16 {
        match screen {
            Screen::Button(id) => self.locate_button(id).0,
            Screen::Lcd(id) => self.locate_lcd(id).0,
        }
    }

    /// The hardware image id and image format to use for a screen, or `None` when this model
    /// doesn't have that screen. A screen on any page up to [`MAX_PAGES`] resolves to the physical
    /// screen it is shown on, whether or not that page is showing or even exists yet.
    pub fn resolve_screen(&self, screen: Screen) -> Option<(u8, ImageFormat)> {
        match screen {
            Screen::Button(id) => {
                let (page, key) = self.locate_button(id);

                if page < MAX_PAGES && (key as usize) < self.screen_key_count() {
                    Some((key, self.image_format()))
                } else {
                    None
                }
            }
            Screen::Lcd(id) => {
                let (page, segment) = self.locate_lcd(id);

                if page < MAX_PAGES {
                    self.lcd_hw_key(segment)
                        .map(|key| (key, self.lcd_image_format()))
                } else {
                    None
                }
            }
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

    /// Human readable button name, used as the subtype of the Home Assistant trigger. Buttons with
    /// a name of their own say which page they are on past the first, since the name is the same on
    /// every page.
    pub fn button_label(&self, id: u16) -> String {
        let (page, key) = self.locate_button(id);

        let name = match (self, key) {
            (Kind::VsdInsideN1, 15) => "Top button left",
            (Kind::VsdInsideN1, 16) => "Top button right",
            _ => return format!("Button {id}"),
        };

        match page {
            0 => name.to_string(),
            _ => format!("{name} (page {page})"),
        }
    }

    pub fn knob_label(&self, id: u8) -> String {
        format!("Knob {id}")
    }

    /// Human readable name of a button or knob, the same one its triggers use
    pub fn control_label(&self, control: Control) -> String {
        match control {
            Control::Button(id) => self.button_label(id),
            Control::Knob(id) => self.knob_label(id),
        }
    }

    /// Every knob, and every button on the pages in `pages`
    pub fn controls(&self, pages: Range<u16>) -> Vec<Control> {
        let knobs = (0..self.encoder_count() as u8).map(Control::Knob);
        let buttons = pages
            .flat_map(move |page| self.page_buttons(page))
            .map(Control::Button);

        knobs.chain(buttons).collect()
    }
}

/// Splits an id numbered on across pages into its page and its place on that page. A model with
/// none of something has every id on the first page, where none of them resolve anyway.
fn locate(id: u16, per_page: usize) -> (u16, u8) {
    match per_page as u16 {
        0 => (0, id.min(u8::MAX.into()) as u8),
        per_page => (id / per_page, (id % per_page) as u8),
    }
}

/// Names a screen the way it is shown to the user, both in log lines and as the Home Assistant
/// entity it is controlled through
impl std::fmt::Display for Screen {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Screen::Button(id) => write!(f, "Button {id}"),
            Screen::Lcd(id) => write!(f, "LCD segment {id}"),
        }
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

    #[test]
    fn screens_are_listed_per_page_buttons_first() {
        assert_eq!(
            Kind::MiraboxN3.page_screens(0),
            (0..6).map(Screen::Button).collect::<Vec<_>>()
        );
        // The N3's second page starts after all nine of its buttons, not just the six screens
        assert_eq!(
            Kind::MiraboxN3.page_screens(1),
            (9..15).map(Screen::Button).collect::<Vec<_>>()
        );

        let n1 = Kind::VsdInsideN1.page_screens(0);
        assert_eq!(n1.len(), 18);
        assert_eq!(n1[14], Screen::Button(14));
        assert_eq!(n1[15], Screen::Lcd(0));
        assert_eq!(n1[17], Screen::Lcd(2));

        let n1 = Kind::VsdInsideN1.page_screens(1);
        assert_eq!(n1.len(), 18);
        assert_eq!(n1[0], Screen::Button(17));
        assert_eq!(n1[14], Screen::Button(31));
        assert_eq!(n1[15], Screen::Lcd(3));
        assert_eq!(n1[17], Screen::Lcd(5));
    }

    #[test]
    fn lcd_segments_are_numbered_on_across_pages() {
        let n1 = Kind::VsdInsideN1;

        assert_eq!(n1.lcd_id(1, 0), 3);
        assert_eq!(n1.locate_lcd(5), (1, 2));
        assert_eq!(n1.screen_page(Screen::Lcd(5)), 1);
        assert_eq!(n1.screen_page(Screen::Button(16)), 0);

        // A model with no strip doesn't divide by its zero segments
        assert_eq!(Kind::MiraboxN3.screen_page(Screen::Lcd(5)), 0);
    }

    #[test]
    fn buttons_are_numbered_on_across_pages() {
        let n1 = Kind::VsdInsideN1;

        assert_eq!(n1.button_id(0, 16), 16);
        assert_eq!(n1.button_id(1, 0), 17);
        assert_eq!(n1.button_id(2, 3), 37);

        for id in [0, 16, 17, 37, 271] {
            let (page, key) = n1.locate_button(id);
            assert_eq!(n1.button_id(page, key), id);
        }

        assert_eq!(
            n1.page_buttons(1).collect::<Vec<_>>(),
            (17..34).collect::<Vec<_>>()
        );
    }

    #[test]
    fn named_buttons_say_which_page_they_are_on() {
        let n1 = Kind::VsdInsideN1;

        assert_eq!(n1.button_label(15), "Top button left");
        assert_eq!(n1.button_label(17 + 16), "Top button right (page 1)");
        assert_eq!(n1.button_label(17), "Button 17");
    }

    #[test]
    fn only_screens_a_model_has_resolve_to_a_hardware_id() {
        let hw_key = |kind: Kind, screen| kind.resolve_screen(screen).map(|(key, _)| key);

        // Buttons with a screen use their own id, LCD segments continue after them
        assert_eq!(hw_key(Kind::VsdInsideN1, Screen::Button(14)), Some(14));
        assert_eq!(hw_key(Kind::VsdInsideN1, Screen::Lcd(0)), Some(15));

        // Screens on later pages are shown on the same physical screens
        assert_eq!(hw_key(Kind::VsdInsideN1, Screen::Button(17 + 14)), Some(14));
        assert_eq!(hw_key(Kind::MiraboxN3, Screen::Button(9)), Some(0));
        assert_eq!(hw_key(Kind::VsdInsideN1, Screen::Lcd(3 + 2)), Some(17));

        // Buttons 15 and 16 exist on the N1 but have no screen of their own, on any page
        assert_eq!(hw_key(Kind::VsdInsideN1, Screen::Button(15)), None);
        assert_eq!(hw_key(Kind::VsdInsideN1, Screen::Button(17 + 15)), None);
        // And the N3 has neither that many screens per page nor an LCD strip
        assert_eq!(hw_key(Kind::MiraboxN3, Screen::Button(6)), None);
        assert_eq!(hw_key(Kind::MiraboxN3, Screen::Lcd(0)), None);

        // Nor is there a page past the last one there can be
        let past_the_end = Kind::MiraboxN3.button_id(MAX_PAGES, 0);
        assert_eq!(hw_key(Kind::MiraboxN3, Screen::Button(past_the_end)), None);
        let past_the_end = Kind::VsdInsideN1.lcd_id(MAX_PAGES, 0);
        assert_eq!(hw_key(Kind::VsdInsideN1, Screen::Lcd(past_the_end)), None);
    }

    #[test]
    fn every_screen_a_model_lists_can_be_resolved() {
        for kind in [Kind::MiraboxN3, Kind::VsdInsideN1] {
            for screen in (0..MAX_PAGES).flat_map(|page| kind.page_screens(page)) {
                assert!(
                    kind.resolve_screen(screen).is_some(),
                    "{kind:?} lists {screen} but cannot resolve it"
                );
            }
        }
    }
}
