//! Bounded URI rewriting for the supported, unencrypted RFC 8216 playlist subset.
//! Original media tags remain intact; the player still selects variants and renditions.

use std::ops::Range;
use std::time::Duration;

/// Supported resource roles. Format and rendition selection remain the player's job.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ResourceKind {
    Playlist,
    Media,
}

/// Fixed diagnostics never include input playlist data or origin URLs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PlaylistError {
    Invalid,
    Unsupported,
    TooLarge,
}

impl std::fmt::Display for PlaylistError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Invalid => "invalid_hls_playlist",
            Self::Unsupported => "unsupported_hls_playlist",
            Self::TooLarge => "hls_playlist_limit",
        })
    }
}
impl std::error::Error for PlaylistError {}

pub const MAX_PLAYLIST_BYTES: usize = 1024 * 1024;
pub const MAX_LINE_BYTES: usize = 16 * 1024;
pub const MAX_REFERENCES: usize = 512;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Reference {
    pub uri: String,
    pub kind: ResourceKind,
    pub implicit_from: Option<usize>,
    span: Range<usize>,
}

/// Metadata for a complete segment URI, without changing the source representation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MediaSegment {
    pub reference_index: usize,
    pub sequence: u64,
    pub duration: Duration,
    pub discontinuity_sequence: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Playlist {
    text: String,
    references: Vec<Reference>,
    media_timeline: Option<(u64, Option<u64>)>,
    discontinuity_sequence: Option<u64>,
    retention: Duration,
    segments: Vec<MediaSegment>,
    simple_replay: bool,
}

impl Playlist {
    /// Immutable URI spans prevent callers from corrupting the bounded rewrite.
    pub fn references(&self) -> &[Reference] {
        &self.references
    }
    pub fn media_timeline(&self) -> Option<(u64, Option<u64>)> {
        self.media_timeline
    }
    /// Declared discontinuity base, or the RFC 8216 default zero for media playlists.
    /// The count of inline discontinuity tags is not included in this value.
    pub fn discontinuity_sequence(&self) -> Option<u64> {
        self.discontinuity_sequence
    }
    pub fn retention(&self) -> Duration {
        self.retention
    }
    pub fn segments(&self) -> &[MediaSegment] {
        &self.segments
    }
    /// Only self-contained, declared-independent media segments are replay candidates.
    /// The consumer must still validate TS bytes, URI uniqueness and complete responses.
    pub fn supports_simple_replay(&self) -> bool {
        self.simple_replay
    }
    pub fn parse(body: &[u8], retention_cap: Duration) -> Result<Self, PlaylistError> {
        if body.len() > MAX_PLAYLIST_BYTES {
            return Err(PlaylistError::TooLarge);
        }
        let text = std::str::from_utf8(body).map_err(|_| PlaylistError::Invalid)?;
        if text.lines().next() != Some("#EXTM3U") || text.contains('\0') {
            return Err(PlaylistError::Invalid);
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
        let mut discontinuity_sequence = 0_u64;
        let mut discontinuity_sequence_seen = false;
        let mut discontinuities = 0_u64;
        let mut segment_count = 0_u64;
        let mut next_duration = None;
        let mut duration_sum = 0_f64;
        let mut max_duration = 0_f64;
        let mut segments = Vec::new();
        let mut independent = false;
        let mut replay_excluded = false;
        for raw in text.split_inclusive('\n') {
            let line = raw.strip_suffix('\n').unwrap_or(raw);
            let line = line.strip_suffix('\r').unwrap_or(line);
            if line.len() > MAX_LINE_BYTES {
                return Err(PlaylistError::TooLarge);
            }
            if line.bytes().any(|b| b < 0x20 || b == 0x7f) {
                return Err(PlaylistError::Invalid);
            }
            let mut push = |uri: &str, kind: ResourceKind, span: Range<usize>| {
                if references.len() >= MAX_REFERENCES || uri.is_empty() || uri.contains(['{', '}'])
                {
                    return Err(PlaylistError::Unsupported);
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
                    return Err(PlaylistError::Invalid);
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
                    let duration = next_duration.take().ok_or(PlaylistError::Invalid)?;
                    let parsed_duration = Duration::try_from_secs_f64(duration);
                    if let Ok(duration) = parsed_duration {
                        segments.push(MediaSegment {
                            reference_index: references.len() - 1,
                            sequence: first_sequence
                                .checked_add(segment_count)
                                .ok_or(PlaylistError::Invalid)?,
                            duration,
                            discontinuity_sequence: discontinuity_sequence
                                .checked_add(discontinuities)
                                .ok_or(PlaylistError::Invalid)?,
                        });
                    } else {
                        replay_excluded = true;
                    }
                    duration_sum += duration;
                    max_duration = max_duration.max(duration);
                    if !duration_sum.is_finite() {
                        return Err(PlaylistError::Invalid);
                    }
                    segment_count = segment_count.checked_add(1).ok_or(PlaylistError::Invalid)?;
                    let index = references.len() - 1;
                    previous_range = if let Some((length, offset)) = pending_range.take() {
                        let start = match offset {
                            Some(offset) => offset,
                            None => {
                                let (previous, end) =
                                    previous_range.ok_or(PlaylistError::Invalid)?;
                                references[index].implicit_from = Some(previous);
                                end
                            }
                        };
                        Some((
                            index,
                            start.checked_add(length).ok_or(PlaylistError::Invalid)?,
                        ))
                    } else {
                        None
                    };
                }
            } else if line == "#EXT-X-INDEPENDENT-SEGMENTS" {
                if independent {
                    return Err(PlaylistError::Invalid);
                }
                independent = true;
            } else if line == "#EXT-X-GAP" || line == "#EXT-X-I-FRAMES-ONLY" {
                replay_excluded = true;
            } else if line == "#EXT-X-DISCONTINUITY" {
                media = true;
                discontinuities = discontinuities
                    .checked_add(1)
                    .ok_or(PlaylistError::Invalid)?;
                discontinuity_sequence
                    .checked_add(discontinuities)
                    .ok_or(PlaylistError::Invalid)?;
            } else if line == "#EXT-X-DISCONTINUITY-SEQUENCE" {
                return Err(PlaylistError::Invalid);
            } else if let Some((tag, attributes)) = line.split_once(':') {
                match tag {
                    "#EXT-X-STREAM-INF" => {
                        if next_playlist {
                            return Err(PlaylistError::Invalid);
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
                                return Err(PlaylistError::Invalid);
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
                            return Err(PlaylistError::Invalid);
                        }
                    }
                    "#EXT-X-MAP" => {
                        replay_excluded = true;
                        media = true;
                        let attrs = parse_attributes(attributes)?;
                        let uri = attrs
                            .iter()
                            .find(|a| a.name == "URI")
                            .ok_or(PlaylistError::Invalid)?;
                        if !uri.quoted {
                            return Err(PlaylistError::Invalid);
                        }
                        if let Some(range) = attrs.iter().find(|a| a.name == "BYTERANGE") {
                            if !range.quoted {
                                return Err(PlaylistError::Invalid);
                            }
                            let (length, start) = parse_byte_range(range.value)?;
                            // An implicit MAP offset has no unambiguous initialization
                            // resource predecessor in the supported relay subset.
                            let start = start.ok_or(PlaylistError::Unsupported)?;
                            start.checked_add(length).ok_or(PlaylistError::Invalid)?;
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
                            return Err(PlaylistError::Unsupported);
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
                        return Err(PlaylistError::Unsupported);
                    }
                    "#EXT-X-BYTERANGE" => {
                        replay_excluded = true;
                        media = true;
                        if pending_range.is_some() {
                            return Err(PlaylistError::Invalid);
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
                            return Err(PlaylistError::Invalid);
                        }
                        first_sequence = attributes.parse().map_err(|_| PlaylistError::Invalid)?;
                        sequence_seen = true;
                    }
                    "#EXT-X-DISCONTINUITY-SEQUENCE" => {
                        media = true;
                        if discontinuity_sequence_seen
                            || discontinuities != 0
                            || segment_count != 0
                            || attributes.is_empty()
                            || attributes.len() > 20
                            || !attributes.bytes().all(|byte| byte.is_ascii_digit())
                        {
                            return Err(PlaylistError::Invalid);
                        }
                        discontinuity_sequence =
                            attributes.parse().map_err(|_| PlaylistError::Invalid)?;
                        discontinuity_sequence_seen = true;
                    }
                    "#EXT-X-DISCONTINUITY" => return Err(PlaylistError::Invalid),
                    "#EXTINF" => {
                        media = true;
                        let duration = attributes.split(',').next().unwrap_or_default();
                        if next_duration.is_some()
                            || duration.is_empty()
                            || !duration
                                .bytes()
                                .all(|byte| byte.is_ascii_digit() || byte == b'.')
                        {
                            return Err(PlaylistError::Invalid);
                        }
                        let duration = duration
                            .parse::<f64>()
                            .map_err(|_| PlaylistError::Invalid)?;
                        if !duration.is_finite() || duration <= 0.0 {
                            return Err(PlaylistError::Invalid);
                        }
                        next_duration = Some(duration);
                    }
                    "#EXT-X-DATERANGE" => {
                        media = true;
                        let attrs = parse_attributes(attributes)?;
                        if attrs.iter().any(|attribute| {
                            attribute.name == "URI"
                                || attribute.name.ends_with("-URI")
                                || attribute.name == "X-ASSET-LIST"
                                || (attribute.name == "CLASS"
                                    && attribute.value == "com.apple.hls.interstitial")
                        }) {
                            // Interstitial assets need their own managed lifetime and
                            // resource rewriting. Do not silently leave origin URLs
                            // in an otherwise managed playlist.
                            return Err(PlaylistError::Unsupported);
                        }
                    }
                    "#EXT-X-TARGETDURATION"
                    | "#EXT-X-PROGRAM-DATE-TIME"
                    | "#EXT-X-PLAYLIST-TYPE" => {
                        media = true;
                    }
                    _ if attributes.contains("URI=") => {
                        // Do not accidentally leave an unknown resource reference pointing
                        // outside the relay or claim support for an extension we cannot parse.
                        return Err(PlaylistError::Unsupported);
                    }
                    _ => {}
                }
            }
            if master && media {
                return Err(PlaylistError::Invalid);
            }
            offset += raw.len();
        }
        if next_playlist
            || pending_range.is_some()
            || next_duration.is_some()
            || (references.is_empty() && !media)
        {
            return Err(PlaylistError::Invalid);
        }
        let seconds = duration_sum + max_duration;
        if !seconds.is_finite() {
            return Err(PlaylistError::Invalid);
        }
        let retention = Duration::try_from_secs_f64(seconds.min(retention_cap.as_secs_f64()))
            .map_err(|_| PlaylistError::Invalid)?;
        Ok(Self {
            text: text.to_owned(),
            references,
            retention,
            segments,
            simple_replay: media && !master && independent && !replay_excluded,
            discontinuity_sequence: media.then_some(discontinuity_sequence),
            media_timeline: if media {
                Some((
                    first_sequence,
                    segment_count
                        .checked_sub(1)
                        .map(|count| {
                            first_sequence
                                .checked_add(count)
                                .ok_or(PlaylistError::Invalid)
                        })
                        .transpose()?,
                ))
            } else {
                None
            },
        })
    }

    pub fn render(self, replacements: &[String]) -> Result<String, PlaylistError> {
        if self.references.len() != replacements.len() {
            return Err(PlaylistError::Invalid);
        }
        let mut output_len = self.text.len();
        for (reference, replacement) in self.references.iter().zip(replacements) {
            output_len = output_len
                .checked_sub(reference.span.len())
                .and_then(|length| length.checked_add(replacement.len()))
                .ok_or(PlaylistError::TooLarge)?;
        }
        if output_len > MAX_PLAYLIST_BYTES {
            return Err(PlaylistError::TooLarge);
        }
        let mut text = String::with_capacity(output_len);
        let mut offset = 0;
        for (reference, replacement) in self.references.iter().zip(replacements) {
            text.push_str(&self.text[offset..reference.span.start]);
            text.push_str(replacement);
            offset = reference.span.end;
            if text.len() > MAX_PLAYLIST_BYTES {
                return Err(PlaylistError::TooLarge);
            }
        }
        text.push_str(&self.text[offset..]);
        if text.len() > MAX_PLAYLIST_BYTES {
            return Err(PlaylistError::TooLarge);
        }
        Ok(text)
    }
}

fn parse_byte_range(value: &str) -> Result<(u64, Option<u64>), PlaylistError> {
    let (length, start) = value
        .split_once('@')
        .map_or((value, None), |(length, start)| (length, Some(start)));
    let parse = |value: &str| {
        if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
            return Err(PlaylistError::Invalid);
        }
        value.parse::<u64>().map_err(|_| PlaylistError::Invalid)
    };
    let length = parse(length)?;
    if length == 0 {
        return Err(PlaylistError::Invalid);
    }
    Ok((length, start.map(parse).transpose()?))
}

struct Attribute<'a> {
    name: &'a str,
    value: &'a str,
    quoted: bool,
    span: Range<usize>,
}

fn parse_attributes(value: &str) -> Result<Vec<Attribute<'_>>, PlaylistError> {
    let mut result: Vec<Attribute<'_>> = Vec::new();
    let mut position = 0;
    while position < value.len() {
        let equals = value[position..].find('=').ok_or(PlaylistError::Invalid)? + position;
        let name = &value[position..equals];
        if name.is_empty()
            || !name
                .bytes()
                .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'-')
            || result.iter().any(|a| a.name == name)
        {
            return Err(PlaylistError::Invalid);
        }
        position = equals + 1;
        let quoted = value.as_bytes().get(position) == Some(&b'"');
        if quoted {
            position += 1;
        }
        let start = position;
        let end = if quoted {
            value[position..].find('"').ok_or(PlaylistError::Invalid)? + position
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
            return Err(PlaylistError::Invalid);
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
            return Err(PlaylistError::Invalid);
        }
        position += 1;
    }
    if result.is_empty() {
        return Err(PlaylistError::Invalid);
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn parse(body: &[u8]) -> Result<Playlist, PlaylistError> {
        Playlist::parse(body, Duration::from_secs(6 * 60 * 60))
    }

    #[test]
    fn master_preserves_attributes_and_rewrites_every_rendition() {
        let source = "#EXTM3U\n#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"a\",NAME=\"one,two\",URI=\"audio?sig=a%2Fb\"\n#EXT-X-I-FRAME-STREAM-INF:BANDWIDTH=128000,URI=\"iframe\"\n#EXT-X-STREAM-INF:BANDWIDTH=1000000,AUDIO=\"a\"\nvideo?sig=x%2Fy\n";
        let p = parse(source.as_bytes()).unwrap();
        assert_eq!(p.references.len(), 3);
        assert!(p
            .references
            .iter()
            .all(|r| r.kind == ResourceKind::Playlist));
        assert_eq!(p.references[0].uri, "audio?sig=a%2Fb");
        let result = p
            .render(&["/r/1".into(), "/r/2".into(), "/r/3".into()])
            .unwrap();
        let result = result.as_str();
        assert!(result.contains("NAME=\"one,two\",URI=\"/r/1\""));
        assert!(result.ends_with("\n/r/3\n"));
    }

    #[test]
    fn media_preserves_map_ranges_epochs_and_endlist() {
        let source = "#EXTM3U\r\n#EXT-X-MEDIA-SEQUENCE:10\r\n#EXT-X-MAP:URI=\"file.mp4\",BYTERANGE=\"20@0\"\r\n#EXT-X-KEY:METHOD=NONE\r\n#EXT-X-DISCONTINUITY\r\n#EXTINF:2.000,\r\n#EXT-X-BYTERANGE:50@20\r\nfile.mp4\r\n#EXT-X-ENDLIST\r\n";
        let p = parse(source.as_bytes()).unwrap();
        assert_eq!(p.references.len(), 2);
        let body = p.render(&["/r/1".into(), "/r/1".into()]).unwrap();
        let body = body.as_str();
        assert!(body.contains("BYTERANGE=\"20@0\""));
        assert!(body.contains("#EXT-X-DISCONTINUITY\r\n"));
        assert!(body.ends_with("/r/1\r\n#EXT-X-ENDLIST\r\n"));
    }

    #[test]
    fn byte_ranges_require_a_valid_predecessor_and_checked_offsets() {
        let source = "#EXTM3U\n#EXTINF:1,\n#EXT-X-BYTERANGE:4@0\nfile.ts\n#EXTINF:1,\n#EXT-X-BYTERANGE:4\n./file.ts\n";
        let playlist = parse(source.as_bytes()).unwrap();
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
            assert!(parse(source.as_bytes()).is_err(), "{source}");
        }
    }

    #[test]
    fn empty_live_playlist_and_empty_quoted_metadata_are_valid() {
        assert!(parse(b"#EXTM3U\n#EXT-X-TARGETDURATION:2\n").is_ok());
        assert!(parse(b"#EXTM3U\n#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"a\",NAME=\"\"\n#EXT-X-STREAM-INF:BANDWIDTH=1\na.m3u8\n").is_ok());
        assert!(parse(b"#EXTM3U\n#EXT-X-MAP:URI=\"\"\na.ts\n").is_err());
        assert!(parse(b"#EXTM3U\n# comment\tcontrol\na.ts\n").is_err());
    }

    #[test]
    fn duration_and_sequence_arithmetic_are_bounded() {
        let playlist = parse(b"#EXTM3U\n#EXTINF:1.5,\na.ts\n#EXTINF:2.5,\nb.ts\n").unwrap();
        assert_eq!(playlist.retention, Duration::from_millis(6500));
        for source in [
            "#EXTM3U\na.ts\n",
            "#EXTM3U\n#EXTINF:NaN,\na.ts\n",
            "#EXTM3U\n#EXTINF:0,\na.ts\n",
            "#EXTM3U\n#EXTINF:-1,\na.ts\n",
            "#EXTM3U\n#EXTINF:1e300,\na.ts\n",
            "#EXTM3U\n#EXTINF:1,\n#EXTINF:1,\na.ts\n",
            "#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:18446744073709551615\n#EXTINF:1,\na.ts\n#EXTINF:1,\nb.ts\n",
        ] { assert!(parse(source.as_bytes()).is_err(), "{source}"); }
    }

    #[test]
    fn discontinuity_sequence_is_bounded_unique_and_before_segments_or_discontinuities() {
        for tag in [
            "#EXT-X-DISCONTINUITY-SEQUENCE",
            "#EXT-X-DISCONTINUITY-SEQUENCE:",
            "#EXT-X-DISCONTINUITY-SEQUENCE:-1",
            "#EXT-X-DISCONTINUITY-SEQUENCE:+1",
            "#EXT-X-DISCONTINUITY-SEQUENCE:1.0",
            "#EXT-X-DISCONTINUITY-SEQUENCE: 1",
            "#EXT-X-DISCONTINUITY-SEQUENCE:one",
            "#EXT-X-DISCONTINUITY-SEQUENCE:18446744073709551616",
            "#EXT-X-DISCONTINUITY-SEQUENCE:000000000000000000000",
            "#EXT-X-DISCONTINUITY-SEQUENCE:1\n#EXT-X-DISCONTINUITY-SEQUENCE:1",
            "#EXT-X-DISCONTINUITY\n#EXT-X-DISCONTINUITY-SEQUENCE:1",
            "#EXTINF:1,\nprevious.ts\n#EXT-X-DISCONTINUITY-SEQUENCE:1",
            "#EXT-X-DISCONTINUITY-SEQUENCE:18446744073709551615\n#EXT-X-DISCONTINUITY",
            "#EXT-X-DISCONTINUITY-SEQUENCE:18446744073709551614\n#EXT-X-DISCONTINUITY\n#EXT-X-DISCONTINUITY",
            "#EXT-X-DISCONTINUITY:1",
        ] {
            let source = format!("#EXTM3U\n{tag}\n#EXTINF:1,\nmedia.ts\n");
            assert_eq!(parse(source.as_bytes()), Err(PlaylistError::Invalid), "{tag}");
        }
    }

    #[test]
    fn discontinuity_base_defaults_to_zero_and_rewriting_preserves_exact_tags() {
        let default = parse(b"#EXTM3U\n#EXT-X-DISCONTINUITY\n#EXTINF:1,\nmedia.ts\n").unwrap();
        assert_eq!(default.discontinuity_sequence(), Some(0));
        let master = parse(b"#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1\nvideo.m3u8\n").unwrap();
        assert_eq!(master.discontinuity_sequence(), None);
        for prefix in ["#EXTINF:1,\n", "#EXTINF:1,\n#EXT-X-BYTERANGE:4@0\n"] {
            let source = format!("#EXTM3U\n{prefix}#EXT-X-DISCONTINUITY-SEQUENCE:1\nmedia.ts\n");
            let parsed = parse(source.as_bytes()).unwrap();
            assert_eq!(parsed.discontinuity_sequence(), Some(1));
            assert_eq!(parsed.render(&["media.ts".into()]).unwrap(), source);
        }
        for (base, inline) in [
            (0, "#EXT-X-DISCONTINUITY\r\n"),
            (u64::MAX, ""),
            (u64::MAX - 1, "#EXT-X-DISCONTINUITY\r\n"),
        ] {
            let source = format!("#EXTM3U\r\n#EXT-X-DISCONTINUITY-SEQUENCE:{base}\r\n{inline}#EXTINF:1,\r\nmedia.ts\r\n");
            let parsed = parse(source.as_bytes()).unwrap();
            assert_eq!(parsed.discontinuity_sequence(), Some(base));
            assert_eq!(parsed.clone().render(&["media.ts".into()]).unwrap(), source);
            let rendered = parsed.render(&["/local/1.ts".into()]).unwrap();
            assert_eq!(
                rendered,
                source.replace("\r\nmedia.ts\r\n", "\r\n/local/1.ts\r\n")
            );
            assert_eq!(
                parse(rendered.as_bytes()).unwrap().discontinuity_sequence(),
                Some(base)
            );
        }
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
            assert!(parse(source.as_bytes()).is_err(), "{source}");
        }
    }

    #[test]
    fn limits_cover_reference_count_and_line_lengths() {
        assert!(matches!(
            parse(format!("#EXTM3U\n{}\n", "a".repeat(MAX_LINE_BYTES + 1)).as_bytes()),
            Err(PlaylistError::TooLarge)
        ));
        assert!(
            parse(format!("#EXTM3U\n{}", "a.ts\n".repeat(MAX_REFERENCES + 1)).as_bytes()).is_err()
        );
    }

    #[test]
    fn ordinary_dateranges_are_preserved_but_asset_references_are_unsupported() {
        let source = "#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXT-X-DATERANGE:ID=\"event\",CLASS=\"synthetic\",START-DATE=\"2026-01-01T00:00:00Z\",DURATION=1.0,X-NOTE=\"text URI=metadata\"\n#EXTINF:1,\na.ts\n";
        let parsed = parse(source.as_bytes()).unwrap();
        let rewritten = parsed.render(&["/local/1.ts".into()]).unwrap();
        assert_eq!(rewritten, source.replace("\na.ts\n", "\n/local/1.ts\n"));
        for attributes in [
            "X-ASSET-URI=\"https://example.invalid/asset.m3u8\"",
            "X-ASSET-LIST=\"https://example.invalid/list.json\"",
            "X-CUSTOM-URI=\"https://example.invalid/resource\"",
            "CLASS=\"com.apple.hls.interstitial\"",
        ] {
            let source = format!("#EXTM3U\n#EXT-X-DATERANGE:ID=\"event\",START-DATE=\"2026-01-01T00:00:00Z\",{attributes}\n#EXTINF:1,\na.ts\n");
            assert_eq!(parse(source.as_bytes()), Err(PlaylistError::Unsupported));
        }
    }

    #[test]
    fn public_parser_and_renderer_have_deterministic_bounded_outputs() {
        let source = "#EXTM3U\n#EXTINF:1,\nmedia/日本.ts\n";
        let parsed = Playlist::parse(source.as_bytes(), Duration::from_millis(50)).unwrap();
        assert_eq!(parsed.retention(), Duration::from_millis(50));
        assert_eq!(
            parsed,
            Playlist::parse(source.as_bytes(), Duration::from_millis(50)).unwrap()
        );
        assert_eq!(parsed.media_timeline(), Some((0, Some(0))));
        let original: Vec<_> = parsed.references().iter().map(|r| r.uri.clone()).collect();
        assert_eq!(parsed.clone().render(&original).unwrap(), source);
        assert_eq!(parsed.clone().render(&[]), Err(PlaylistError::Invalid));
        assert_eq!(
            parsed.render(&["x".repeat(MAX_PLAYLIST_BYTES)]),
            Err(PlaylistError::TooLarge)
        );
        assert_eq!(
            parse(b"#EXTM3U\n#EXT-X-MAP:URI=\"file.mp4\",BYTERANGE=\"4\"\n#EXTINF:1,\nfile.mp4\n"),
            Err(PlaylistError::Unsupported)
        );
    }

    #[test]
    fn retained_replay_metadata_preserves_sequences_durations_and_discontinuities() {
        let source = "#EXTM3U\n#EXT-X-INDEPENDENT-SEGMENTS\n#EXT-X-MEDIA-SEQUENCE:12\n#EXT-X-DISCONTINUITY-SEQUENCE:7\n#EXTINF:1.25,\na.ts\n#EXT-X-DISCONTINUITY\n#EXTINF:2,\nb.ts\n";
        let parsed = parse(source.as_bytes()).unwrap();
        assert!(parsed.supports_simple_replay());
        assert_eq!(
            parsed.segments(),
            &[
                MediaSegment {
                    reference_index: 0,
                    sequence: 12,
                    duration: Duration::from_millis(1250),
                    discontinuity_sequence: 7
                },
                MediaSegment {
                    reference_index: 1,
                    sequence: 13,
                    duration: Duration::from_secs(2),
                    discontinuity_sequence: 8
                },
            ]
        );
        assert!(!parse(
            source
                .replace("#EXT-X-INDEPENDENT-SEGMENTS\n", "")
                .as_bytes()
        )
        .unwrap()
        .supports_simple_replay());
        for tag in [
            "#EXT-X-MAP:URI=\"init.mp4\"",
            "#EXT-X-BYTERANGE:4@0",
            "#EXT-X-GAP",
            "#EXT-X-I-FRAMES-ONLY",
        ] {
            let source =
                format!("#EXTM3U\n#EXT-X-INDEPENDENT-SEGMENTS\n{tag}\n#EXTINF:1,\nmedia.ts\n");
            assert!(!parse(source.as_bytes()).unwrap().supports_simple_replay());
        }
    }
}
