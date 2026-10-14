//! Validate the JSONB layout used by Lance 13 / jsonb 0.5 before SDK decoding.
//! Its decoder assumes valid UTF-8 and can panic on malformed numeric payloads.
//! Walk borrowed payloads; never materialize JSON rows or invoke that decoder.
use crate::error::Failure;

pub(crate) fn validate(bytes: &[u8]) -> Result<(), Failure> {
    container(bytes, 0).map_err(|message| Failure::from(format!("Invalid JSONB input: {message}")))
}

fn container(bytes: &[u8], depth: usize) -> Result<(), &'static str> {
    if depth > 64 {
        return Err("nesting exceeds 64 levels");
    }
    let header = word(bytes)?;
    let count = (header & 0x1fff_ffff) as usize;
    let object = header & 0xe000_0000 == 0x4000_0000;
    let entries = match header & 0xe000_0000 {
        0x2000_0000 if count == 0 => 1,
        0x8000_0000 => count,
        0x4000_0000 => count.checked_mul(2).ok_or("entry count overflow")?,
        _ => return Err("invalid container header"),
    };
    let start = entries
        .checked_mul(4)
        .and_then(|n| n.checked_add(4))
        .ok_or("entry count overflow")?;
    let headers = bytes.get(4..start).ok_or("truncated entry headers")?;
    let mut payload = bytes.get(start..).ok_or("truncated payload")?;
    let mut previous_key = None;
    for (index, header) in headers.as_chunks::<4>().0.iter().enumerate() {
        let entry = word(header)?;
        // The pinned decoder supports lengths, not the JSONB offset flag.
        if entry & 0x8000_0000 != 0 {
            return Err("offset entries are unsupported");
        }
        let length = (entry & 0x0fff_ffff) as usize;
        let value = payload.get(..length).ok_or("truncated entry payload")?;
        payload = &payload[length..];
        let kind = entry & 0x7000_0000;
        if object && index < count {
            if kind != 0x1000_0000 {
                return Err("object keys must be strings");
            }
            let key = std::str::from_utf8(value).map_err(|_| "object key is not UTF-8")?;
            if previous_key.is_some_and(|previous| previous >= key) {
                return Err("object keys must be unique and sorted");
            }
            previous_key = Some(key);
        } else {
            match kind {
                0x0000_0000 | 0x3000_0000 | 0x4000_0000 if value.is_empty() => {}
                0x1000_0000 => {
                    std::str::from_utf8(value).map_err(|_| "string is not UTF-8")?;
                }
                0x2000_0000 => number(value)?,
                0x5000_0000 => container(value, depth + 1)?,
                _ => return Err("invalid JSON value tag or length"),
            }
        }
    }
    if !payload.is_empty() {
        return Err("trailing payload bytes");
    }
    Ok(())
}

fn word(bytes: &[u8]) -> Result<u32, &'static str> {
    let bytes = bytes.get(..4).ok_or("truncated header")?;
    Ok(u32::from_be_bytes(bytes.try_into().unwrap()))
}

fn number(bytes: &[u8]) -> Result<(), &'static str> {
    match (bytes.first(), bytes.len()) {
        (Some(0x00), 1) => Ok(()),
        (Some(0x40 | 0x50), 2 | 3 | 5 | 9) => Ok(()),
        (Some(0x60), 9) => {
            let value = f64::from_be_bytes(bytes[1..].try_into().unwrap());
            if value.is_finite() {
                Ok(())
            } else {
                Err("numbers must be finite")
            }
        }
        // Current decimal layouts and the decoder's two legacy precision-byte layouts.
        (Some(0x70), 10 | 18 | 19 | 34 | 35) => Ok(()),
        _ => Err("invalid or non-finite number"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encoder_output_is_valid_and_truncated_payloads_fail_without_panics() {
        for text in [
            "null",
            "true",
            "false",
            "0",
            "-9223372036854775808",
            "18446744073709551615",
            "1.5",
            "1e100",
            "\"snow 雪\"",
            "[]",
            "{}",
            "{\"a\":[1,null,{\"b\":\"雪\"}],\"z\":false}",
        ] {
            let bytes = lance_arrow::json::encode_json(text).unwrap();
            assert!(validate(&bytes).is_ok(), "{text}");
            for length in 0..bytes.len() {
                assert!(validate(&bytes[..length]).is_err(), "{text} at {length}");
            }
            let mut trailing = bytes;
            trailing.push(0);
            assert!(validate(&trailing).is_err());
        }
    }

    #[test]
    fn malformed_strings_keys_numbers_and_depth_are_rejected() {
        for bytes in [
            vec![0x20, 0, 0, 0, 0x10, 0, 0, 1, 0xff],
            vec![0x40, 0, 0, 1, 0x10, 0, 0, 1, 0, 0, 0, 0, 0xff],
            vec![0x20, 0, 0, 0, 0x20, 0, 0, 0],
            vec![0x20, 0, 0, 0, 0x20, 0, 0, 1, 0x10],
            vec![0x20, 0, 0, 0, 0x20, 0, 0, 1, 0x20],
            vec![0x20, 0, 0, 0, 0x20, 0, 0, 1, 0x60],
            vec![
                0x20, 0, 0, 0, 0x20, 0, 0, 9, 0x60, 0x7f, 0xf0, 0, 0, 0, 0, 0, 0,
            ],
            vec![
                0x40, 0, 0, 2, 0x10, 0, 0, 1, 0x10, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, b'a', b'a',
            ],
            vec![
                0x40, 0, 0, 2, 0x10, 0, 0, 1, 0x10, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, b'b', b'a',
            ],
            vec![0x20, 0, 0, 0, 0x80, 0, 0, 0],
            vec![0x80, 0, 0, 0xff],
        ] {
            assert!(validate(&bytes).is_err());
        }
        let mut nested = lance_arrow::json::encode_json("null").unwrap();
        for _ in 0..65 {
            let mut parent = vec![0x80, 0, 0, 1];
            parent.extend_from_slice(&(0x5000_0000 | nested.len() as u32).to_be_bytes());
            parent.extend_from_slice(&nested);
            nested = parent;
        }
        assert!(validate(&nested).is_err());
    }
}
