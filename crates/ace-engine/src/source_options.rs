//! Preserve unsupported selection intent instead of silently playing a different file.

pub(crate) fn validate<'a>(
    params: impl IntoIterator<Item = (&'a str, &'a str)>,
    uri: bool,
) -> Result<(), &'static str> {
    for (key, value) in params {
        match key {
            "direct_url" => return Err("direct_url playback is not supported"),
            "transport_file_data" => return Err("transport_file_data upload is not supported"),
            "index" if uri => validate_index(value)?,
            "_idx" if !uri => validate_index(value)?,
            // URI index and HTTP _idx are distinct client contracts, not aliases.
            "index" | "_idx" => return Err("unsupported file selection parameter"),
            "stream_id" if !uri && value == "-1" => {}
            "stream_id"
            | "playlist_selected_indexes"
            | "playlist_start_from"
            | "next_file_indexes" => return Err("multi-file or stream selection is not supported"),
            _ => {}
        }
    }
    Ok(())
}

fn validate_index(value: &str) -> Result<(), &'static str> {
    if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
        return Err("invalid file index");
    }
    match value.parse::<u32>() {
        Ok(0) => Ok(()),
        Ok(_) => Err("non-default file selection is not supported"),
        Err(_) => Err("invalid file index"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_and_stream_intent_is_never_silently_discarded() {
        for uri in [false, true] {
            assert_eq!(validate([("stream_id", "-1")], uri).is_ok(), !uri);
            let key = if uri { "index" } else { "_idx" };
            assert!(validate([(key, "0")], uri).is_ok());
            for value in ["", "-1", "1", "+0", "0.0", " 0", "4294967296"] {
                assert!(validate([(key, value)], uri).is_err(), "{key}={value}");
            }
            let wrong_key = if uri { "_idx" } else { "index" };
            assert!(validate([(wrong_key, "0")], uri).is_err());
            for key in [
                "direct_url",
                "transport_file_data",
                "stream_id",
                "playlist_selected_indexes",
                "playlist_start_from",
                "next_file_indexes",
            ] {
                assert!(validate([(key, "0")], uri).is_err());
            }
            // The SDK adds wrapper expansion flags even to ordinary descriptors.
            assert!(validate(
                [
                    ("expand_wrapper", "1"),
                    ("mode", "full"),
                    ("dump_transport_file", "1")
                ],
                uri
            )
            .is_ok());
        }
    }
}
