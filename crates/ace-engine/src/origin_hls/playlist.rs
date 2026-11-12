//! Bounded URI rewriting for the supported, unencrypted RFC 8216 playlist subset.
//! Original media tags remain intact; the player still selects variants and renditions.

use super::{OriginError, ResourceKind};
use bytes::Bytes;
use std::ops::Range;
use std::time::Duration;

pub(super) const MAX_PLAYLIST_BYTES: usize = 1024 * 1024;
const MAX_LINE_BYTES: usize = 16 * 1024;
const MAX_REFERENCES: usize = 512;

pub(super) struct Reference {
    pub(super) uri: String,
    pub(super) kind: ResourceKind,
    pub(super) implicit_from: Option<usize>,
    span: Range<usize>,
}

pub(super) struct Playlist {
    text: String,
    pub(super) references: Vec<Reference>,
    pub(super) media_timeline: Option<(u64, Option<u64>)>,
    pub(super) retention: Duration,
}

impl Playlist {
    pub(super) fn parse(body: &[u8]) -> Result<Self, OriginError> {
        if body.len() > MAX_PLAYLIST_BYTES {
            return Err(OriginError::TooLarge);
        }
        let text = std::str::from_utf8(body).map_err(|_| OriginError::InvalidPlaylist)?;
        if text.lines().next() != Some("#EXTM3U") || text.contains('\0') {
            return Err(OriginError::InvalidPlaylist);
        }
        let mut references = Vec::new();
        let mut offset = 0;
        let mut next_playlist = false;
        let mut master = false;
        let mut media = false;
        let mut pending_range: Option<(u64, Option<u64>)> = None;
        let mut previous_range: Option<(usize, u64)> = None;
        let mut first_sequence = 0_u64;
        let mut sequence_seen = false;
        let mut segment_count = 0_u64;
        let mut next_duration = None;
        let mut duration_sum = 0_f64;
        let mut max_duration = 0_f64;
        for raw in text.split_inclusive('\n') {
            let line = raw.strip_suffix('\n').unwrap_or(raw);
            let line = line.strip_suffix('\r').unwrap_or(line);
            if line.len() > MAX_LINE_BYTES {
                return Err(OriginError::TooLarge);
            }
            if line.bytes().any(|b| b < 0x20 || b == 0x7f) {
                return Err(OriginError::InvalidPlaylist);
            }
            let mut push = |uri: &str, kind: ResourceKind, span: Range<usize>| {
                if references.len() >= MAX_REFERENCES || uri.is_empty() || uri.contains(['{', '}'])
                {
                    return Err(OriginError::UnsupportedPlaylist);
                }
                references.push(Reference {
                    uri: uri.to_owned(),
                    kind,
                    implicit_from: None,
                    span: offset + span.start..offset + span.end,
                });
                Ok(())
            };
            if !line.is_empty() && !line.starts_with('#') {
                if line.trim() != line {
                    return Err(OriginError::InvalidPlaylist);
                }
                let kind = if next_playlist {
                    next_playlist = false;
                    ResourceKind::Playlist
                } else {
                    media = true;
                    ResourceKind::Media
                };
                push(line, kind, 0..line.len())?;
                if kind == ResourceKind::Media {
                    let duration = next_duration.take().ok_or(OriginError::InvalidPlaylist)?;
                    duration_sum += duration;
                    max_duration = max_duration.max(duration);
                    if !duration_sum.is_finite() {
                        return Err(OriginError::InvalidPlaylist);
                    }
                    segment_count = segment_count
                        .checked_add(1)
                        .ok_or(OriginError::InvalidPlaylist)?;
                    let index = references.len() - 1;
                    previous_range = if let Some((length, offset)) = pending_range.take() {
                        let start = match offset {
                            Some(offset) => offset,
                            None => {
                                let (previous, end) =
                                    previous_range.ok_or(OriginError::InvalidPlaylist)?;
                                references[index].implicit_from = Some(previous);
                                end
                            }
                        };
                        Some((
                            index,
                            start
                                .checked_add(length)
                                .ok_or(OriginError::InvalidPlaylist)?,
                        ))
                    } else {
                        None
                    };
                }
            } else if let Some((tag, attributes)) = line.split_once(':') {
                match tag {
                    "#EXT-X-STREAM-INF" => {
                        if next_playlist {
                            return Err(OriginError::InvalidPlaylist);
                        }
                        parse_attributes(attributes)?;
                        master = true;
                        next_playlist = true;
                    }
                    "#EXT-X-MEDIA" | "#EXT-X-I-FRAME-STREAM-INF" | "#EXT-X-SESSION-DATA" => {
                        master = true;
                        let attrs = parse_attributes(attributes)?;
                        if let Some(uri) = attrs.iter().find(|a| a.name == "URI") {
                            if !uri.quoted {
                                return Err(OriginError::InvalidPlaylist);
                            }
                            let kind = if tag == "#EXT-X-SESSION-DATA" {
                                ResourceKind::Media
                            } else {
                                ResourceKind::Playlist
                            };
                            let start = tag.len() + 1;
                            push(
                                uri.value,
                                kind,
                                start + uri.span.start..start + uri.span.end,
                            )?;
                        } else if tag == "#EXT-X-I-FRAME-STREAM-INF" {
                            return Err(OriginError::InvalidPlaylist);
                        }
                    }
                    "#EXT-X-MAP" => {
                        media = true;
                        let attrs = parse_attributes(attributes)?;
                        let uri = attrs
                            .iter()
                            .find(|a| a.name == "URI")
                            .ok_or(OriginError::InvalidPlaylist)?;
                        if !uri.quoted {
                            return Err(OriginError::InvalidPlaylist);
                        }
                        if let Some(range) = attrs.iter().find(|a| a.name == "BYTERANGE") {
                            if !range.quoted {
                                return Err(OriginError::InvalidPlaylist);
                            }
                            let (length, start) = parse_byte_range(range.value)?;
                            // An implicit MAP offset has no unambiguous initialization
                            // resource predecessor in the supported relay subset.
                            let start = start.ok_or(OriginError::UnsupportedPlaylist)?;
                            start
                                .checked_add(length)
                                .ok_or(OriginError::InvalidPlaylist)?;
                        }
                        let start = tag.len() + 1;
                        push(
                            uri.value,
                            ResourceKind::Media,
                            start + uri.span.start..start + uri.span.end,
                        )?;
                    }
                    "#EXT-X-KEY" => {
                        media = true;
                        let attrs = parse_attributes(attributes)?;
                        if attrs.len() != 1
                            || attrs[0].name != "METHOD"
                            || attrs[0].value != "NONE"
                            || attrs[0].quoted
                        {
                            return Err(OriginError::UnsupportedPlaylist);
                        }
                    }
                    "#EXT-X-SESSION-KEY"
                    | "#EXT-X-DEFINE"
                    | "#EXT-X-CONTENT-STEERING"
                    | "#EXT-X-PART"
                    | "#EXT-X-PART-INF"
                    | "#EXT-X-PRELOAD-HINT"
                    | "#EXT-X-SERVER-CONTROL"
                    | "#EXT-X-RENDITION-REPORT"
                    | "#EXT-X-SKIP" => {
                        return Err(OriginError::UnsupportedPlaylist);
                    }
                    "#EXT-X-BYTERANGE" => {
                        media = true;
                        if pending_range.is_some() {
                            return Err(OriginError::InvalidPlaylist);
                        }
                        pending_range = Some(parse_byte_range(attributes)?);
                    }
                    "#EXT-X-MEDIA-SEQUENCE" => {
                        media = true;
                        if sequence_seen
                            || segment_count != 0
                            || attributes.is_empty()
                            || !attributes.bytes().all(|byte| byte.is_ascii_digit())
                        {
                            return Err(OriginError::InvalidPlaylist);
                        }
                        first_sequence = attributes
                            .parse()
                            .map_err(|_| OriginError::InvalidPlaylist)?;
                        sequence_seen = true;
                    }
                    "#EXTINF" => {
                        media = true;
                        let duration = attributes.split(',').next().unwrap_or_default();
                        if next_duration.is_some()
                            || duration.is_empty()
                            || !duration
                                .bytes()
                                .all(|byte| byte.is_ascii_digit() || byte == b'.')
                        {
                            return Err(OriginError::InvalidPlaylist);
                        }
                        let duration = duration
                            .parse::<f64>()
                            .map_err(|_| OriginError::InvalidPlaylist)?;
                        if !duration.is_finite() || duration <= 0.0 {
                            return Err(OriginError::InvalidPlaylist);
                        }
                        next_duration = Some(duration);
                    }
                    "#EXT-X-TARGETDURATION"
                    | "#EXT-X-DISCONTINUITY-SEQUENCE"
                    | "#EXT-X-PROGRAM-DATE-TIME"
                    | "#EXT-X-DATERANGE"
                    | "#EXT-X-PLAYLIST-TYPE" => {
                        media = true;
                    }
                    _ if attributes.contains("URI=") => {
                        // Do not accidentally leave an unknown resource reference pointing
                        // outside the relay or claim support for an extension we cannot parse.
                        return Err(OriginError::UnsupportedPlaylist);
                    }
                    _ => {}
                }
            }
            if master && media {
                return Err(OriginError::InvalidPlaylist);
            }
            offset += raw.len();
        }
        if next_playlist
            || pending_range.is_some()
            || next_duration.is_some()
            || (references.is_empty() && !media)
        {
            return Err(OriginError::InvalidPlaylist);
        }
        let seconds = duration_sum + max_duration;
        if !seconds.is_finite() {
            return Err(OriginError::InvalidPlaylist);
        }
        let retention = Duration::try_from_secs_f64(seconds.min(super::HARD_TIMEOUT.as_secs_f64()))
            .map_err(|_| OriginError::InvalidPlaylist)?;
        Ok(Self {
            text: text.to_owned(),
            references,
            retention,
            media_timeline: if media {
                Some((
                    first_sequence,
                    segment_count
                        .checked_sub(1)
                        .map(|count| {
                            first_sequence
                                .checked_add(count)
                                .ok_or(OriginError::InvalidPlaylist)
                        })
                        .transpose()?,
                ))
            } else {
                None
            },
        })
    }

    pub(super) fn render(self, replacements: &[String]) -> Result<Bytes, OriginError> {
        if self.references.len() != replacements.len() {
            return Err(OriginError::InvalidPlaylist);
        }
        let mut text = String::with_capacity(self.text.len());
        let mut offset = 0;
        for (reference, replacement) in self.references.iter().zip(replacements) {
            text.push_str(&self.text[offset..reference.span.start]);
            text.push_str(replacement);
            offset = reference.span.end;
            if text.len() > MAX_PLAYLIST_BYTES {
                return Err(OriginError::TooLarge);
            }
        }
        text.push_str(&self.text[offset..]);
        if text.len() > MAX_PLAYLIST_BYTES {
            return Err(OriginError::TooLarge);
        }
        Ok(Bytes::from(text))
    }
}

fn parse_byte_range(value: &str) -> Result<(u64, Option<u64>), OriginError> {
    let (length, start) = value
        .split_once('@')
        .map_or((value, None), |(length, start)| (length, Some(start)));
    let parse = |value: &str| {
        if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
            return Err(OriginError::InvalidPlaylist);
        }
        value
            .parse::<u64>()
            .map_err(|_| OriginError::InvalidPlaylist)
    };
    let length = parse(length)?;
    if length == 0 {
        return Err(OriginError::InvalidPlaylist);
    }
    Ok((length, start.map(parse).transpose()?))
}

struct Attribute<'a> {
    name: &'a str,
    value: &'a str,
    quoted: bool,
    span: Range<usize>,
}

fn parse_attributes(value: &str) -> Result<Vec<Attribute<'_>>, OriginError> {
    let mut result: Vec<Attribute<'_>> = Vec::new();
    let mut position = 0;
    while position < value.len() {
        let equals = value[position..]
            .find('=')
            .ok_or(OriginError::InvalidPlaylist)?
            + position;
        let name = &value[position..equals];
        if name.is_empty()
            || !name
                .bytes()
                .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'-')
            || result.iter().any(|a| a.name == name)
        {
            return Err(OriginError::InvalidPlaylist);
        }
        position = equals + 1;
        let quoted = value.as_bytes().get(position) == Some(&b'"');
        if quoted {
            position += 1;
        }
        let start = position;
        let end = if quoted {
            value[position..]
                .find('"')
                .ok_or(OriginError::InvalidPlaylist)?
                + position
        } else {
            value[position..]
                .find(',')
                .map_or(value.len(), |i| position + i)
        };
        let content = &value[start..end];
        if (content.is_empty() && !quoted)
            || content.bytes().any(|b| b < 0x20)
            || (!quoted
                && content
                    .bytes()
                    .any(|b| b.is_ascii_whitespace() || b == b'"'))
        {
            return Err(OriginError::InvalidPlaylist);
        }
        result.push(Attribute {
            name,
            value: content,
            quoted,
            span: start..end,
        });
        position = end + usize::from(quoted);
        if position == value.len() {
            break;
        }
        if value.as_bytes().get(position) != Some(&b',') || position + 1 == value.len() {
            return Err(OriginError::InvalidPlaylist);
        }
        position += 1;
    }
    if result.is_empty() {
        return Err(OriginError::InvalidPlaylist);
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn master_preserves_attributes_and_rewrites_every_rendition() {
        let source = "#EXTM3U\n#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"a\",NAME=\"one,two\",URI=\"audio?sig=a%2Fb\"\n#EXT-X-I-FRAME-STREAM-INF:BANDWIDTH=128000,URI=\"iframe\"\n#EXT-X-STREAM-INF:BANDWIDTH=1000000,AUDIO=\"a\"\nvideo?sig=x%2Fy\n";
        let p = Playlist::parse(source.as_bytes()).unwrap();
        assert_eq!(p.references.len(), 3);
        assert!(p
            .references
            .iter()
            .all(|r| r.kind == ResourceKind::Playlist));
        assert_eq!(p.references[0].uri, "audio?sig=a%2Fb");
        let result = p
            .render(&["/r/1".into(), "/r/2".into(), "/r/3".into()])
            .unwrap();
        let result = std::str::from_utf8(&result).unwrap();
        assert!(result.contains("NAME=\"one,two\",URI=\"/r/1\""));
        assert!(result.ends_with("\n/r/3\n"));
    }

    #[test]
    fn media_preserves_map_ranges_epochs_and_endlist() {
        let source = "#EXTM3U\r\n#EXT-X-MEDIA-SEQUENCE:10\r\n#EXT-X-MAP:URI=\"file.mp4\",BYTERANGE=\"20@0\"\r\n#EXT-X-KEY:METHOD=NONE\r\n#EXT-X-DISCONTINUITY\r\n#EXTINF:2.000,\r\n#EXT-X-BYTERANGE:50@20\r\nfile.mp4\r\n#EXT-X-ENDLIST\r\n";
        let p = Playlist::parse(source.as_bytes()).unwrap();
        assert_eq!(p.references.len(), 2);
        let body = p.render(&["/r/1".into(), "/r/1".into()]).unwrap();
        let body = std::str::from_utf8(&body).unwrap();
        assert!(body.contains("BYTERANGE=\"20@0\""));
        assert!(body.contains("#EXT-X-DISCONTINUITY\r\n"));
        assert!(body.ends_with("/r/1\r\n#EXT-X-ENDLIST\r\n"));
    }

    #[test]
    fn byte_ranges_require_a_valid_predecessor_and_checked_offsets() {
        let source = "#EXTM3U\n#EXTINF:1,\n#EXT-X-BYTERANGE:4@0\nfile.ts\n#EXTINF:1,\n#EXT-X-BYTERANGE:4\n./file.ts\n";
        let playlist = Playlist::parse(source.as_bytes()).unwrap();
        assert_eq!(playlist.references[1].implicit_from, Some(0));
        for source in [
            "#EXTM3U\n#EXT-X-BYTERANGE:4\nfile.ts\n",
            "#EXTM3U\nfile.ts\n#EXT-X-BYTERANGE:4\nfile.ts\n",
            "#EXTM3U\n#EXT-X-BYTERANGE:4@0\n",
            "#EXTM3U\n#EXT-X-BYTERANGE:0@0\nfile.ts\n",
            "#EXTM3U\n#EXT-X-BYTERANGE:4@18446744073709551615\nfile.ts\n",
            "#EXTM3U\n#EXT-X-BYTERANGE:4@0\n#EXT-X-BYTERANGE:4@0\nfile.ts\n",
            "#EXTM3U\n#EXT-X-MAP:URI=\"file\",BYTERANGE=\"4\"\nfile.ts\n",
        ] {
            assert!(Playlist::parse(source.as_bytes()).is_err(), "{source}");
        }
    }

    #[test]
    fn empty_live_playlist_and_empty_quoted_metadata_are_valid() {
        assert!(Playlist::parse(b"#EXTM3U\n#EXT-X-TARGETDURATION:2\n").is_ok());
        assert!(Playlist::parse(b"#EXTM3U\n#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"a\",NAME=\"\"\n#EXT-X-STREAM-INF:BANDWIDTH=1\na.m3u8\n").is_ok());
        assert!(Playlist::parse(b"#EXTM3U\n#EXT-X-MAP:URI=\"\"\na.ts\n").is_err());
        assert!(Playlist::parse(b"#EXTM3U\n# comment\tcontrol\na.ts\n").is_err());
    }

    #[test]
    fn duration_and_sequence_arithmetic_are_bounded() {
        let playlist =
            Playlist::parse(b"#EXTM3U\n#EXTINF:1.5,\na.ts\n#EXTINF:2.5,\nb.ts\n").unwrap();
        assert_eq!(playlist.retention, Duration::from_millis(6500));
        for source in [
            "#EXTM3U\na.ts\n",
            "#EXTM3U\n#EXTINF:NaN,\na.ts\n",
            "#EXTM3U\n#EXTINF:0,\na.ts\n",
            "#EXTM3U\n#EXTINF:-1,\na.ts\n",
            "#EXTM3U\n#EXTINF:1e300,\na.ts\n",
            "#EXTM3U\n#EXTINF:1,\n#EXTINF:1,\na.ts\n",
            "#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:18446744073709551615\n#EXTINF:1,\na.ts\n#EXTINF:1,\nb.ts\n",
        ] { assert!(Playlist::parse(source.as_bytes()).is_err(), "{source}"); }
    }

    #[test]
    fn malformed_or_unsupported_playlists_fail_without_partial_rewrite() {
        for source in [
            "not a playlist",
            "\u{feff}#EXTM3U\na.ts\n",
            "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1\n",
            "#EXTM3U\n#EXT-X-MAP:URI=unquoted\na.mp4\n",
            "#EXTM3U\n#EXT-X-MAP:URI=\"a\",URI=\"b\"\na.mp4\n",
            "#EXTM3U\n#EXT-X-KEY:METHOD=AES-128,URI=\"key\"\na.ts\n",
            "#EXTM3U\n#EXT-X-DEFINE:NAME=\"x\",VALUE=\"y\"\na.ts\n",
            "#EXTM3U\n#EXT-X-PART:DURATION=1,URI=\"a\"\na.ts\n",
            "#EXTM3U\n#EXT-X-UNKNOWN:URI=\"a\"\na.ts\n",
            "#EXTM3U\n{$variable}.ts\n",
            "#EXTM3U\n#EXTINF:1,\n#EXT-X-STREAM-INF:BANDWIDTH=1\na.ts\n",
        ] {
            assert!(Playlist::parse(source.as_bytes()).is_err(), "{source}");
        }
    }

    #[test]
    fn limits_cover_reference_count_and_line_lengths() {
        assert!(matches!(
            Playlist::parse(format!("#EXTM3U\n{}\n", "a".repeat(MAX_LINE_BYTES + 1)).as_bytes()),
            Err(OriginError::TooLarge)
        ));
        assert!(Playlist::parse(
            format!("#EXTM3U\n{}", "a.ts\n".repeat(MAX_REFERENCES + 1)).as_bytes()
        )
        .is_err());
    }
}
