use base64::{Engine, engine::general_purpose::STANDARD};
use image::DynamicImage;
use std::{
    ffi::OsStr,
    fmt,
    path::{Path, PathBuf},
};

/// Directory an icon referenced by name is loaded from. Commands can only name a file directly
/// inside it, so nothing arriving over MQTT can reach the rest of the filesystem.
const ICON_DIR: &str = "images";

/// Reported as a screen's state while it shows an image that arrived over MQTT rather than one
/// of the files in `images/`, which has no name to report. A command carrying it back is
/// ignored, so submitting the Home Assistant text field unchanged leaves the screen alone.
pub const INLINE: &str = "<image>";

/// Blanks a screen, exactly as an empty payload does, for anywhere sending nothing at all is
/// awkward. Matched ignoring case, so `Empty` works too, and it takes precedence over a file of
/// the same name in `images/`.
const EMPTY: &str = "empty";

/// A name longer than this is cut short in error messages, so a mistyped command carrying a
/// whole image doesn't fill the log with it
const MAX_REPORTED_NAME: usize = 64;

/// What an incoming image command asks a screen to do
pub enum Icon {
    /// Draw this image. `state` is the value reported back to Home Assistant: the icon's file
    /// name, or [`INLINE`] for an image that came in over MQTT.
    Show { image: DynamicImage, state: String },
    /// Blank the screen
    Clear,
    /// The marker for an inline image, handed straight back to us; leave the screen alone
    Unchanged,
}

#[derive(Debug)]
pub enum Error {
    /// An icon name that isn't a plain file directly inside `images/`
    UnsafeName(String),
    /// A named file that couldn't be read, or isn't an image
    File(PathBuf, image::ImageError),
    /// A payload that is neither an icon name nor image data
    Unrecognized,
}

/// Resolves the payload of an image command. Anything that decodes as an image is drawn as is,
/// everything else names a file in `images/`, so Home Assistant can send either without having
/// to say which it is.
pub fn from_payload(payload: &[u8]) -> Result<Icon, Error> {
    if payload.is_empty() {
        return Ok(Icon::Clear);
    }

    // Image data published straight onto the topic as bytes
    if let Ok(image) = image::load_from_memory(payload) {
        return Ok(inline(image));
    }

    // Anything else has to be text: base64 image data, or the name of a file in `images/`
    let Ok(text) = std::str::from_utf8(payload) else {
        return Err(Error::Unrecognized);
    };

    match text.trim() {
        "" => Ok(Icon::Clear),
        name if name.eq_ignore_ascii_case(EMPTY) => Ok(Icon::Clear),
        INLINE => Ok(Icon::Unchanged),
        name => match decode_base64(name) {
            Some(image) => Ok(inline(image)),
            None => Ok(Icon::Show {
                image: from_file(name)?,
                state: name.to_string(),
            }),
        },
    }
}

/// Loads an icon out of `images/` by file name
pub fn from_file(name: &str) -> Result<DynamicImage, Error> {
    // `file_name` strips off any directories, so a name that survives it unchanged is one that
    // stays inside `images/`. It also rejects `..` and `.`, which have no file name at all.
    if Path::new(name).file_name() != Some(OsStr::new(name)) {
        return Err(Error::UnsafeName(name.to_string()));
    }

    let path = Path::new(ICON_DIR).join(name);

    image::open(&path).map_err(|e| Error::File(path, e))
}

fn inline(image: DynamicImage) -> Icon {
    Icon::Show {
        image,
        state: INLINE.to_string(),
    }
}

/// Home Assistant templates produce text, so image data built in an automation arrives base64
/// encoded, sometimes as a data URI and sometimes wrapped across several lines. An icon name
/// such as `light.png` isn't valid base64, so it rules itself out here and is treated as a name.
fn decode_base64(text: &str) -> Option<DynamicImage> {
    let encoded = match text.split_once("base64,") {
        Some((prefix, encoded)) if prefix.starts_with("data:") => encoded,
        _ => text,
    };

    let encoded: String = encoded.chars().filter(|c| !c.is_whitespace()).collect();
    let bytes = STANDARD.decode(&encoded).ok()?;

    image::load_from_memory(&bytes).ok()
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::UnsafeName(name) => write!(
                f,
                "'{}' is not the name of a file in {ICON_DIR}/",
                Shortened(name)
            ),
            Error::File(path, e) => write!(f, "cannot read {}: {e}", path.display()),
            Error::Unrecognized => {
                write!(f, "payload is neither an icon name nor an image")
            }
        }
    }
}

/// Prints the start of a payload that is too long to belong in a log line, so a message someone
/// sent by mistake is still reported without a picture's worth of it ending up in the log
pub struct Shortened<'a>(pub &'a str);

impl fmt::Display for Shortened<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0.char_indices().nth(MAX_REPORTED_NAME) {
            Some((end, _)) => write!(f, "{}...", &self.0[..end]),
            None => write!(f, "{}", self.0),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The icon that ships with the repo, used here as a stand-in for any real image file.
    /// Tests run with the crate root as the working directory, which is also where the app
    /// resolves `images/` from.
    const EXISTING_ICON: &str = "tux.png";

    fn png_bytes() -> Vec<u8> {
        std::fs::read(Path::new(ICON_DIR).join(EXISTING_ICON)).expect("test icon should exist")
    }

    fn state_of(icon: Icon) -> String {
        match icon {
            Icon::Show { state, .. } => state,
            Icon::Clear => panic!("expected an image, got a clear"),
            Icon::Unchanged => panic!("expected an image, got no change"),
        }
    }

    #[test]
    fn a_name_loads_the_matching_file_and_reports_it_back() {
        let icon = from_payload(EXISTING_ICON.as_bytes()).expect("icon should load");

        assert_eq!(state_of(icon), EXISTING_ICON);
    }

    #[test]
    fn surrounding_whitespace_in_a_name_is_ignored() {
        let icon = from_payload(format!("  {EXISTING_ICON}\n").as_bytes()).expect("should load");

        assert_eq!(state_of(icon), EXISTING_ICON);
    }

    #[test]
    fn raw_image_data_is_drawn_as_is() {
        let icon = from_payload(&png_bytes()).expect("image should decode");

        // There is no file name to report for an image that arrived over MQTT
        assert_eq!(state_of(icon), INLINE);
    }

    #[test]
    fn base64_image_data_is_drawn_as_is() {
        let encoded = STANDARD.encode(png_bytes());
        let icon = from_payload(encoded.as_bytes()).expect("image should decode");

        assert_eq!(state_of(icon), INLINE);
    }

    #[test]
    fn base64_survives_a_data_uri_and_line_wrapping() {
        let encoded = STANDARD.encode(png_bytes());

        let wrapped = encoded
            .as_bytes()
            .chunks(76)
            .map(|line| std::str::from_utf8(line).unwrap())
            .collect::<Vec<_>>()
            .join("\n");
        assert_eq!(state_of(from_payload(wrapped.as_bytes()).unwrap()), INLINE);

        let data_uri = format!("data:image/png;base64,{encoded}");
        assert_eq!(state_of(from_payload(data_uri.as_bytes()).unwrap()), INLINE);
    }

    #[test]
    fn an_empty_payload_blanks_the_screen() {
        for payload in ["", "   ", "\n"] {
            assert!(matches!(from_payload(payload.as_bytes()), Ok(Icon::Clear)));
        }
    }

    #[test]
    fn the_word_empty_blanks_the_screen_too() {
        for payload in ["empty", "Empty", " EMPTY\n"] {
            assert!(
                matches!(from_payload(payload.as_bytes()), Ok(Icon::Clear)),
                "{payload} should have blanked the screen"
            );
        }
    }

    #[test]
    fn the_inline_marker_handed_back_leaves_the_screen_alone() {
        assert!(matches!(
            from_payload(INLINE.as_bytes()),
            Ok(Icon::Unchanged)
        ));
    }

    #[test]
    fn a_name_cannot_reach_outside_the_icon_directory() {
        for name in ["../config.toml", "/etc/passwd", "..", ".", "sub/dir.png"] {
            assert!(
                matches!(from_payload(name.as_bytes()), Err(Error::UnsafeName(_))),
                "{name} should have been rejected"
            );
        }
    }

    #[test]
    fn a_name_with_no_file_behind_it_is_an_error() {
        assert!(matches!(
            from_payload(b"definitely-not-here.png"),
            Err(Error::File(..))
        ));
    }

    #[test]
    fn binary_that_is_not_an_image_is_rejected() {
        // Invalid UTF-8, so it can't be a name either
        assert!(matches!(
            from_payload(&[0xff, 0xfe, 0x00, 0x01]),
            Err(Error::Unrecognized)
        ));
    }

    #[test]
    fn a_long_name_is_cut_short_when_reported() {
        let name = format!("../{}", "a".repeat(500));
        let message = Error::UnsafeName(name.clone()).to_string();

        assert!(
            message.len() < name.len() / 2,
            "name was not cut short: {message}"
        );
        assert!(message.contains("..."));
    }
}
