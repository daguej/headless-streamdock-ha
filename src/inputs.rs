use mirajazz::{error::MirajazzError, types::DeviceInput};

// The N3 part of this file is slightly modified from
// https://github.com/4ndv/opendeck-akp03/blob/main/src/inputs.rs
// and the N1 part from https://github.com/rattenjunge-samu/opendeck-vsd-n1

const N3_ENCODER_COUNT: usize = 3;
const N3_KEY_COUNT: usize = 9;

const N1_ENCODER_COUNT: usize = 1;
const N1_KEY_COUNT: usize = 17;

pub fn process_input_n3(input: u8, state: u8) -> Result<DeviceInput, MirajazzError> {
    match input {
        (0..=6) | 0x25 | 0x30 | 0x31 => read_button_press_n3(input, state),
        0x90 | 0x91 | 0x50 | 0x51 | 0x60 | 0x61 => read_encoder_value_n3(input),
        0x33..=0x35 => read_encoder_press_n3(input, state),
        _ => Err(MirajazzError::BadData),
    }
}

pub fn process_input_n1(input: u8, state: u8) -> Result<DeviceInput, MirajazzError> {
    // The N1 periodically emits this non-input status frame
    if input == 0xcc && state == 0xff {
        return Ok(DeviceInput::NoData);
    }

    let decoded = match input {
        (0x01..=0x0f) | 0x1e | 0x1f => read_button_press_n1(input, state),
        0x32 | 0x33 => read_encoder_value_n1(input),
        0x23 => read_encoder_press_n1(state),
        _ => Err(MirajazzError::BadData),
    };

    // The N1 emits frames we don't know about, ignore them instead of killing the reader
    if decoded.is_err() {
        println!("Ignoring unknown N1 input: code=0x{input:02x} state=0x{state:02x}");

        return Ok(DeviceInput::NoData);
    }

    decoded
}

/// Turns a 1-based report into the button states expected by mirajazz, where index 0 is
/// the first button
fn read_button_states(states: &[u8], key_count: usize) -> Vec<bool> {
    let mut bools = vec![];

    for i in 0..key_count {
        bools.push(states[i + 1] != 0);
    }

    bools
}

fn read_button_press_n3(input: u8, state: u8) -> Result<DeviceInput, MirajazzError> {
    let mut button_states = vec![0x01];
    button_states.extend(vec![0u8; N3_KEY_COUNT + 1]);

    if input == 0 {
        return Ok(DeviceInput::ButtonStateChange(read_button_states(
            &button_states,
            N3_KEY_COUNT,
        )));
    }

    let pressed_index: usize = match input {
        // Six buttons with displays
        (1..=6) => input as usize,
        // Three buttons without displays
        0x25 => 7,
        0x30 => 8,
        0x31 => 9,
        _ => return Err(MirajazzError::BadData),
    };

    button_states[pressed_index] = state;

    Ok(DeviceInput::ButtonStateChange(read_button_states(
        &button_states,
        N3_KEY_COUNT,
    )))
}

fn read_encoder_value_n3(input: u8) -> Result<DeviceInput, MirajazzError> {
    let mut encoder_values = vec![0i8; N3_ENCODER_COUNT];

    let (encoder, value): (usize, i8) = match input {
        // Left encoder
        0x90 => (0, -1),
        0x91 => (0, 1),
        // Middle (top) encoder
        0x50 => (1, -1),
        0x51 => (1, 1),
        // Right encoder
        0x60 => (2, -1),
        0x61 => (2, 1),
        _ => return Err(MirajazzError::BadData),
    };

    encoder_values[encoder] = value;
    Ok(DeviceInput::EncoderTwist(encoder_values))
}

fn read_encoder_press_n3(input: u8, state: u8) -> Result<DeviceInput, MirajazzError> {
    let mut encoder_states = vec![false; N3_ENCODER_COUNT];

    let encoder: usize = match input {
        0x33 => 0, // Left encoder
        0x35 => 1, // Middle (top) encoder
        0x34 => 2, // Right encoder
        _ => return Err(MirajazzError::BadData),
    };

    encoder_states[encoder] = state != 0;
    Ok(DeviceInput::EncoderStateChange(encoder_states))
}

fn read_button_press_n1(input: u8, state: u8) -> Result<DeviceInput, MirajazzError> {
    let mut button_states = vec![0x01];
    button_states.extend(vec![0u8; N1_KEY_COUNT + 1]);

    let pressed_index: usize = match input {
        // Fifteen keys with displays
        0x01..=0x0f => input as usize,
        // Two buttons without displays, above the LCD strip
        0x1e => 16,
        0x1f => 17,
        _ => return Err(MirajazzError::BadData),
    };

    button_states[pressed_index] = state;

    Ok(DeviceInput::ButtonStateChange(read_button_states(
        &button_states,
        N1_KEY_COUNT,
    )))
}

fn read_encoder_value_n1(input: u8) -> Result<DeviceInput, MirajazzError> {
    let mut encoder_values = vec![0i8; N1_ENCODER_COUNT];

    encoder_values[0] = match input {
        0x32 => -1,
        0x33 => 1,
        _ => return Err(MirajazzError::BadData),
    };

    Ok(DeviceInput::EncoderTwist(encoder_values))
}

fn read_encoder_press_n1(state: u8) -> Result<DeviceInput, MirajazzError> {
    Ok(DeviceInput::EncoderStateChange(vec![state != 0]))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn buttons(input: DeviceInput) -> Vec<bool> {
        match input {
            DeviceInput::ButtonStateChange(states) => states,
            other => panic!("expected a button state change, got {other:?}"),
        }
    }

    fn pressed(input: DeviceInput) -> Vec<usize> {
        buttons(input)
            .iter()
            .enumerate()
            .filter(|(_, state)| **state)
            .map(|(i, _)| i)
            .collect()
    }

    fn encoders(input: DeviceInput) -> Vec<bool> {
        match input {
            DeviceInput::EncoderStateChange(states) => states,
            other => panic!("expected an encoder state change, got {other:?}"),
        }
    }

    fn twist(input: DeviceInput) -> Vec<i8> {
        match input {
            DeviceInput::EncoderTwist(values) => values,
            other => panic!("expected an encoder twist, got {other:?}"),
        }
    }

    #[test]
    fn n3_buttons_are_zero_indexed() {
        assert_eq!(buttons(process_input_n3(1, 1).unwrap()).len(), N3_KEY_COUNT);
        // Display keys
        assert_eq!(pressed(process_input_n3(1, 1).unwrap()), vec![0]);
        assert_eq!(pressed(process_input_n3(6, 1).unwrap()), vec![5]);
        // Keys without a display
        assert_eq!(pressed(process_input_n3(0x25, 1).unwrap()), vec![6]);
        assert_eq!(pressed(process_input_n3(0x31, 1).unwrap()), vec![8]);
        // Release reports no button held down
        assert_eq!(
            pressed(process_input_n3(1, 0).unwrap()),
            Vec::<usize>::new()
        );
    }

    #[test]
    fn n3_encoders() {
        assert_eq!(twist(process_input_n3(0x90, 1).unwrap()), vec![-1, 0, 0]);
        assert_eq!(twist(process_input_n3(0x51, 1).unwrap()), vec![0, 1, 0]);
        assert_eq!(twist(process_input_n3(0x60, 1).unwrap()), vec![0, 0, -1]);
        assert_eq!(
            encoders(process_input_n3(0x35, 1).unwrap()),
            vec![false, true, false]
        );
    }

    #[test]
    fn n1_buttons_are_zero_indexed() {
        assert_eq!(buttons(process_input_n1(1, 1).unwrap()).len(), N1_KEY_COUNT);
        // Display keys
        assert_eq!(pressed(process_input_n1(0x01, 1).unwrap()), vec![0]);
        assert_eq!(pressed(process_input_n1(0x0f, 1).unwrap()), vec![14]);
        // Top buttons, which have no display
        assert_eq!(pressed(process_input_n1(0x1e, 1).unwrap()), vec![15]);
        assert_eq!(pressed(process_input_n1(0x1f, 1).unwrap()), vec![16]);
        // Release reports no button held down
        assert_eq!(
            pressed(process_input_n1(0x0f, 0).unwrap()),
            Vec::<usize>::new()
        );
    }

    #[test]
    fn n1_encoder() {
        assert_eq!(twist(process_input_n1(0x32, 1).unwrap()), vec![-1]);
        assert_eq!(twist(process_input_n1(0x33, 1).unwrap()), vec![1]);
        assert_eq!(encoders(process_input_n1(0x23, 1).unwrap()), vec![true]);
        assert_eq!(encoders(process_input_n1(0x23, 0).unwrap()), vec![false]);
    }

    #[test]
    fn n1_ignores_noise() {
        // Status frame the device emits on its own
        assert!(process_input_n1(0xcc, 0xff).unwrap().is_empty());
        // Anything else we don't recognize is ignored rather than fatal
        assert!(process_input_n1(0x7f, 0x01).unwrap().is_empty());
    }
}
