//! Conservative, single-packet PSI normalization for live HLS only.
//! Unsupported PSI disables selection; malformed/non-current tables never replace good metadata.
use crate::mpegts::{mpeg_crc32, ts_pid, ts_timing};

const N: usize = 188;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AudioCodec {
    AdtsAac,
    LatmAac,
    MpegAudio,
    Ac3,
    Eac3,
    Dts,
}

/// Descriptor loops must be validated even when their contents are not interpreted.
fn descriptors(mut bytes: &[u8]) -> Option<Option<AudioCodec>> {
    let mut codec = None;
    while !bytes.is_empty() {
        let len = *bytes.get(1)? as usize;
        let body = bytes.get(2..2 + len)?;
        codec = codec.or(match bytes[0] {
            0x6a if body
                .first()
                .is_some_and(|flags| body.len() > (flags & 0xf0).count_ones() as usize) =>
            {
                Some(AudioCodec::Ac3)
            }
            0x7a if body
                .first()
                .is_some_and(|flags| body.len() > (flags & 0xf7).count_ones() as usize) =>
            {
                Some(AudioCodec::Eac3)
            }
            0x7b if body.len() >= 5 => Some(AudioCodec::Dts),
            0x05 if body.len() >= 4 => match &body[..4] {
                b"AC-3" => Some(AudioCodec::Ac3),
                b"EAC3" => Some(AudioCodec::Eac3),
                b"DTS1" | b"DTS2" | b"DTS3" => Some(AudioCodec::Dts),
                _ => None,
            },
            _ => None,
        });
        bytes = &bytes[2 + len..];
    }
    Some(codec)
}

fn audio_codec(ty: u8, desc: &[u8]) -> Option<Option<AudioCodec>> {
    let private = descriptors(desc)?;
    Some(match ty {
        0x0f => Some(AudioCodec::AdtsAac),
        0x11 => Some(AudioCodec::LatmAac),
        0x03 | 0x04 => Some(AudioCodec::MpegAudio),
        0x06 => private,
        _ => None,
    })
}

#[derive(Debug)]
enum PsiError {
    Invalid,
    Unsupported,
}

fn payload(packet: &[u8; N]) -> Option<usize> {
    match packet[3] & 0x30 {
        0x10 => Some(4),
        0x30 => (5 + (packet[4] as usize) < N).then_some(5 + packet[4] as usize),
        _ => None,
    }
}

fn section(packet: &[u8; N], id: u8, min: usize, program: Option<u16>) -> Result<&[u8], PsiError> {
    use PsiError::*;
    if packet[0] != 0x47 || packet[1] & 0x80 != 0 || packet[3] & 0xc0 != 0 {
        return Err(Invalid);
    }
    let off = payload(packet).ok_or(Invalid)?;
    if packet[1] & 0x40 == 0 {
        return Err(Unsupported);
    }
    let start = off + 1 + packet[off] as usize;
    let header = packet.get(start..start + 3).ok_or(Invalid)?;
    if header[0] != id || header[1] & 0xf0 != 0xb0 {
        return Err(Invalid);
    }
    let len = (((header[1] & 15) as usize) << 8) | header[2] as usize;
    if len < min || len > 1021 {
        return Err(Invalid);
    }
    // Reject a header for another program or a future table before classifying a fragment as
    // unsupported. Neither is evidence that our current program changed transport layout.
    if packet.get(start + 5).is_none_or(|flags| flags & 1 == 0) {
        return Err(Invalid);
    }
    if let Some(program) = program {
        let bytes = packet.get(start + 3..start + 5).ok_or(Invalid)?;
        if u16::from_be_bytes([bytes[0], bytes[1]]) != program {
            return Err(Invalid);
        }
    }
    let end = start + 3 + len;
    let s = packet.get(start..end).ok_or(Unsupported)?;
    if s[5] & 1 == 0 || mpeg_crc32(s) != 0 {
        return Err(Invalid);
    }
    if s[6] != 0 || s[7] != 0 || packet[off] != 0 || packet[end..].iter().any(|b| *b != 0xff) {
        return Err(Unsupported);
    }
    Ok(s)
}

fn pid(bytes: &[u8]) -> u16 {
    ((bytes[0] as u16 & 31) << 8) | bytes[1] as u16
}

/// Result of observing one source packet. `changed` requires discarding an unfinished HLS run
/// and refreshing the video/table cache from `tables` before accepting another access point.
pub struct AudioPacket {
    pub packet: Option<[u8; N]>,
    pub changed: bool,
}

#[derive(Default)]
pub struct LiveAudioSelection {
    mapping: Option<(u16, u16)>,
    transport_stream_id: Option<u16>,
    pat: Option<[u8; N]>,
    pmt: Option<[u8; N]>,
    source_pmt: Option<Vec<u8>>,
    removed: Vec<u16>,
    pcr: Option<u16>,
    effective: Option<Vec<u8>>,
    version: u8,
    rewrite: bool,
    output_cc: u8,
}

impl LiveAudioSelection {
    pub fn reset(&mut self) {
        *self = Self::default();
    }

    pub fn pcr_pid(&self) -> Option<u16> {
        self.pcr
    }

    pub fn tables(&self) -> impl Iterator<Item = &[u8; N]> {
        self.pat.iter().chain(self.pmt.iter())
    }

    fn disable(&mut self) -> bool {
        let changed = self.pmt.is_some() || !self.removed.is_empty();
        self.pmt = None;
        self.source_pmt = None;
        self.removed.clear();
        self.pcr = None;
        self.effective = None;
        self.rewrite = false;
        changed
    }

    pub fn observe(&mut self, mut packet: [u8; N]) -> AudioPacket {
        let source_pid = ts_pid(&packet);
        let mut changed = false;
        if source_pid == 0 {
            match section(&packet, 0, 9, None) {
                Ok(s) => {
                    let entries = &s[8..s.len() - 4];
                    if entries.len() % 4 != 0 {
                        return AudioPacket {
                            packet: None,
                            changed: false,
                        };
                    }
                    let programs: Vec<_> = entries
                        .chunks_exact(4)
                        .filter(|e| e[0] != 0 || e[1] != 0)
                        .map(|e| (u16::from_be_bytes([e[0], e[1]]), pid(&e[2..])))
                        .collect();
                    if programs.len() != 1 {
                        changed = self.disable();
                        self.mapping = None;
                        self.transport_stream_id = None;
                        self.pat = None;
                    } else {
                        let transport_stream_id = u16::from_be_bytes([s[3], s[4]]);
                        if self.mapping != Some(programs[0])
                            || self.transport_stream_id != Some(transport_stream_id)
                        {
                            changed = self.disable();
                            self.mapping = Some(programs[0]);
                            self.transport_stream_id = Some(transport_stream_id);
                        }
                        self.pat = Some(packet);
                    }
                }
                Err(PsiError::Unsupported) => {
                    changed = self.disable();
                    self.mapping = None;
                    self.transport_stream_id = None;
                    self.pat = None;
                }
                Err(PsiError::Invalid) => {
                    // Preserve legacy passthrough until strict PSI has been established.
                    if self.pat.is_some() {
                        return AudioPacket {
                            packet: None,
                            changed: false,
                        };
                    }
                }
            }
        } else if self.mapping.is_some_and(|(_, p)| p == source_pid) && payload(&packet).is_some() {
            match self.update_pmt(&packet) {
                Ok(different) => {
                    changed = different;
                    if let Some(pmt) = self.pmt {
                        packet = pmt;
                    }
                }
                Err(PsiError::Unsupported) => changed = self.disable(),
                Err(PsiError::Invalid) => {
                    return AudioPacket {
                        packet: None,
                        changed: false,
                    }
                }
            }
        } else if self.removed.contains(&source_pid) {
            let timing = ts_timing(&packet);
            if Some(source_pid) == self.pcr && (timing.pcr.is_some() || timing.discontinuity) {
                // Retain the full bounded adaptation field, replacing PES with stuffing.
                let len = packet[4] as usize;
                if packet[3] & 0x20 == 0 || len == 0 || len > 183 {
                    return AudioPacket {
                        packet: None,
                        changed,
                    };
                }
                packet[1] &= !0x40;
                packet[3] = 0x20;
                packet[4] = 183;
                packet[5 + len..].fill(0xff);
            } else {
                return AudioPacket {
                    packet: None,
                    changed,
                };
            }
        }
        AudioPacket {
            packet: Some(packet),
            changed,
        }
    }

    fn update_pmt(&mut self, packet: &[u8; N]) -> Result<bool, PsiError> {
        use PsiError::*;
        let s = section(packet, 2, 13, self.mapping.map(|m| m.0))?;
        if Some(u16::from_be_bytes([s[3], s[4]])) != self.mapping.map(|m| m.0) {
            return Err(Invalid);
        }
        let end = s.len() - 4;
        let info = (((s[10] & 15) as usize) << 8) | s[11] as usize;
        let begin = 12 + info;
        if begin > end {
            return Err(Invalid);
        }
        descriptors(s.get(12..begin).ok_or(Invalid)?).ok_or(Invalid)?;
        let mut pos = begin;
        let mut entries = Vec::new();
        while pos < end {
            let h = s
                .get(pos..pos + 5)
                .filter(|_| pos + 5 <= end)
                .ok_or(Invalid)?;
            let len = (((h[3] & 15) as usize) << 8) | h[4] as usize;
            let next = pos + 5 + len;
            if next > end {
                return Err(Invalid);
            }
            let codec = audio_codec(h[0], &s[pos + 5..next]).ok_or(Invalid)?;
            let es_pid = pid(&h[1..]);
            if entries.iter().any(|(_, _, p, _)| *p == es_pid) {
                return Err(Unsupported);
            }
            entries.push((pos, next, es_pid, codec));
            pos = next;
        }
        let selected = entries
            .iter()
            .find(|e| e.3 == Some(AudioCodec::AdtsAac))
            .map(|e| e.2);
        let removed: Vec<_> = entries
            .iter()
            .filter(|e| selected.is_some() && e.3.is_some() && Some(e.2) != selected)
            .map(|e| e.2)
            .collect();
        let mut out = s[..begin].to_vec();
        for (a, b, p, _) in entries {
            if !removed.contains(&p) {
                out.extend_from_slice(&s[a..b]);
            }
        }
        let len = out.len() + 1; // excludes first three bytes, includes CRC
        out[1] = 0xb0 | ((len >> 8) as u8 & 15);
        out[2] = len as u8;
        let upstream_version = (s[5] >> 1) & 31;
        out[5] &= 0xc1; // effective content comparison excludes upstream version and CRC
        let mut source_content = s[..end].to_vec();
        source_content[5] &= 0xc1;
        let different = self.source_pmt.as_ref() != Some(&source_content);
        if self.effective.as_ref() != Some(&out) {
            self.version = if self.effective.is_some() {
                (self.version + 1) & 31
            } else {
                (upstream_version + u8::from(!removed.is_empty())) & 31
            };
        }
        self.effective = Some(out.clone());
        self.rewrite = selected.is_some();
        if !self.rewrite {
            // Passthrough restores the source version too; use it as the baseline if AAC returns.
            self.version = upstream_version;
        }
        out[5] |= self.version << 1;
        let crc = mpeg_crc32(&out);
        out.extend_from_slice(&crc.to_be_bytes());
        let mut rewritten = [0xff; N];
        rewritten[..4].copy_from_slice(&packet[..4]);
        rewritten[3] = (rewritten[3] & 0x0f) | 0x10;
        rewritten[4] = 0;
        rewritten[5..5 + out.len()].copy_from_slice(&out);
        self.pmt = Some(if self.rewrite { rewritten } else { *packet });
        self.source_pmt = Some(source_content);
        self.removed = removed;
        self.pcr = Some(pid(&s[8..]));
        Ok(different)
    }

    /// Assign continuity to actual emitted PMT payloads, including injected prefixes.
    pub fn emitted(&mut self, bytes: &mut [u8]) {
        if !self.rewrite {
            return;
        }
        for packet in bytes.chunks_exact_mut(N) {
            if self.mapping.is_some_and(|(_, p)| p == ts_pid(packet)) {
                let cc = if packet[3] & 0x10 != 0 {
                    let cc = self.output_cc;
                    self.output_cc = (self.output_cc + 1) & 15;
                    cc
                } else {
                    self.output_cc.wrapping_sub(1) & 15
                };
                packet[3] = (packet[3] & 0xf0) | cc;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(pid: u16, mut s: Vec<u8>) -> [u8; N] {
        let len = s.len() + 1;
        s[1] = 0xb0 | (len >> 8) as u8;
        s[2] = len as u8;
        s.extend_from_slice(&mpeg_crc32(&s).to_be_bytes());
        let mut p = [0xff; N];
        p[..5].copy_from_slice(&[0x47, 0x40 | (pid >> 8) as u8, pid as u8, 0x10, 0]);
        p[5..5 + s.len()].copy_from_slice(&s);
        p
    }
    fn pat(program: u16, pmt: u16) -> [u8; N] {
        table(
            0,
            vec![
                0,
                0xb0,
                0,
                0,
                1,
                0xc1,
                0,
                0,
                (program >> 8) as u8,
                program as u8,
                0xe0 | (pmt >> 8) as u8,
                pmt as u8,
            ],
        )
    }
    fn pmt(version: u8, entries: &[(u8, u16, &[u8])]) -> [u8; N] {
        let mut s = vec![
            2,
            0xb0,
            0,
            0,
            1,
            0xc1 | version << 1,
            0,
            0,
            0xe1,
            1,
            0xf0,
            3,
            0x52,
            1,
            0xab,
        ];
        for (ty, p, d) in entries {
            s.extend_from_slice(&[*ty, 0xe0 | (p >> 8) as u8, *p as u8, 0xf0, d.len() as u8]);
            s.extend_from_slice(d);
        }
        table(256, s)
    }
    fn source() -> [u8; N] {
        pmt(
            31,
            &[
                (0x1b, 257, &[]),
                (3, 258, &[]),
                (15, 259, &[0x0a, 4, b'e', b'n', b'g', 0]),
            ],
        )
    }
    fn state() -> LiveAudioSelection {
        let mut st = LiveAudioSelection::default();
        st.observe(pat(1, 256));
        st.observe(source());
        st
    }
    fn es_pids(packet: &[u8; N]) -> Vec<u16> {
        let s = section(packet, 2, 13, None).unwrap();
        let mut pos = 12 + (((s[10] & 15) as usize) << 8 | s[11] as usize);
        let mut out = Vec::new();
        while pos < s.len() - 4 {
            out.push(pid(&s[pos + 1..]));
            pos += 5 + (((s[pos + 3] & 15) as usize) << 8 | s[pos + 4] as usize);
        }
        out
    }
    fn elementary(pid: u16) -> [u8; N] {
        let mut p = [0xff; N];
        p[..4].copy_from_slice(&[0x47, (pid >> 8) as u8, pid as u8, 0x10]);
        p
    }
    fn repair(mut p: [u8; N]) -> [u8; N] {
        let end = 8 + (((p[6] & 15) as usize) << 8 | p[7] as usize);
        let crc = mpeg_crc32(&p[5..end - 4]);
        p[end - 4..end].copy_from_slice(&crc.to_be_bytes());
        p
    }

    #[test]
    fn pat_transport_switch_invalidates_selection_but_version_repeat_does_not() {
        let mut st = state();
        let cached = st.pmt;
        let mut repeated = pat(1, 256);
        repeated[10] = 0xc7;
        repeated = repair(repeated);
        assert!(!st.observe(repeated).changed);
        assert_eq!(st.pmt, cached);
        let mut switched = repeated;
        switched[9] = 2; // same program/PMT mapping, different transport_stream_id
        switched = repair(switched);
        assert!(st.observe(switched).changed);
        assert!(st.pmt.is_none());
        assert_eq!(st.pcr_pid(), None);
        assert_eq!(st.tables().copied().collect::<Vec<_>>(), vec![switched]);
        let audio = elementary(258);
        assert_eq!(st.observe(audio).packet, Some(audio));
        let fresh = pmt(17, &[(0x1b, 257, &[]), (3, 258, &[])]);
        assert_eq!(st.observe(fresh).packet, Some(fresh));
        assert_eq!(
            st.tables().copied().collect::<Vec<_>>(),
            vec![switched, fresh]
        );
    }

    #[test]
    fn losing_adts_restores_original_pmt_adaptation_version_and_es_passthrough() {
        let mut st = state();
        let plain = pmt(
            17,
            &[
                (0x1b, 257, &[]),
                (3, 258, &[]),
                (0x11, 259, &[]),
                (6, 260, &[]),
            ],
        );
        let mut original = [0xff; N];
        original[..4].copy_from_slice(&plain[..4]);
        original[3] = 0x39;
        original[4] = 2;
        original[5] = 0;
        original[7..].copy_from_slice(&plain[4..N - 3]);
        let result = st.observe(original);
        assert!(result.changed);
        assert_eq!(result.packet, Some(original));
        assert_eq!(st.pmt, Some(original));
        let mut emitted = original;
        st.emitted(&mut emitted);
        assert_eq!(emitted, original);
        let repeat = st.observe(original);
        assert!(!repeat.changed);
        assert_eq!(repeat.packet, Some(original));
        for pid in [258, 259, 260] {
            let audio = elementary(pid);
            assert_eq!(st.observe(audio).packet, Some(audio));
        }
    }

    #[test]
    fn selection_preserves_program_and_retained_es_descriptors() {
        let mut st = state();
        let out = st.observe(source()).packet.unwrap();
        assert_eq!(es_pids(&out), vec![257, 259]);
        let s = section(&out, 2, 13, None).unwrap();
        assert_eq!(&s[12..15], &[0x52, 1, 0xab]);
        assert_eq!(&s[25..31], &[0x0a, 4, b'e', b'n', b'g', 0]);
        assert_eq!((s[5] >> 1) & 31, 0);
        assert!(st.observe(elementary(258)).packet.is_none());
        assert!(st.observe(elementary(259)).packet.is_some());
    }

    #[test]
    fn private_descriptor_signals_are_bounded_and_unknown_private_survives() {
        let cases: &[(u8, &[u8], bool)] = &[
            (6, &[], true),
            (6, &[0x6a, 0], true),
            (6, &[0x6a, 1, 0], false),
            (6, &[0x7a, 1, 0], false),
            (6, &[0x7b, 5, 0, 0, 0, 0, 0], false),
            (6, &[5, 4, b'A', b'C', b'-', b'3'], false),
            (6, &[5, 4, b'E', b'A', b'C', b'3'], false),
            (6, &[5, 4, b'D', b'T', b'S', b'1'], false),
            (6, &[5, 4, b'N', b'O', b'N', b'E'], true),
            (0x11, &[], false),
        ];
        for (ty, desc, retained) in cases {
            let mut st = LiveAudioSelection::default();
            st.observe(pat(1, 256));
            let out = st
                .observe(pmt(0, &[(15, 259, &[]), (*ty, 258, desc)]))
                .packet
                .unwrap();
            assert_eq!(
                es_pids(&out).contains(&258),
                *retained,
                "descriptor {desc:?}"
            );
        }
    }

    #[test]
    fn invalid_tables_cannot_replace_known_good_selection() {
        let mutations: &[fn(&mut [u8; N])] = &[
            |p| p[25] ^= 1, // CRC corruption
            |p| {
                p[9] = 2;
                *p = repair(*p);
            }, // wrong program
            |p| {
                p[10] &= !1;
                *p = repair(*p);
            }, // not current
            |p| p[4] = 184, // pointer outside packet
            |p| {
                p[18] = 180;
                *p = repair(*p);
            }, // program descriptor bounds
            |p| {
                p[34] = 255;
                *p = repair(*p);
            }, // ES descriptor length bounds
        ];
        for (i, mutate) in mutations.iter().enumerate() {
            let mut st = state();
            let before = st.pmt;
            let mut bad = source();
            mutate(&mut bad);
            let result = st.observe(bad);
            assert!(!result.changed, "mutation {i}");
            assert_eq!(st.pmt, before);
            assert!(st.observe(elementary(258)).packet.is_none());
        }
    }

    #[test]
    fn unsupported_psi_disables_filter_and_drops_cached_rewrite() {
        let mutations: &[fn(&mut [u8; N])] = &[
            |p| p[1] &= !0x40, // continuation
            |p| {
                p[7] = 200;
            }, // fragmented section
            |p| {
                p[11] = 1;
                *p = repair(*p);
            }, // section_number
            |p| {
                p[12] = 1;
                *p = repair(*p);
            }, // last_section_number
            |p| {
                let end = 8 + p[7] as usize;
                p[end] = 0;
            }, // another section after CRC
        ];
        for mutate in mutations {
            let mut st = state();
            let mut bad = source();
            mutate(&mut bad);
            assert!(st.observe(bad).changed);
            assert!(st.pmt.is_none());
            assert!(st.observe(elementary(258)).packet.is_some());
        }
        let mut st = state();
        let multiple = table(
            0,
            vec![0, 0xb0, 0, 0, 1, 0xc1, 0, 0, 0, 1, 0xe1, 0, 0, 2, 0xe2, 0],
        );
        assert!(st.observe(multiple).changed);
        assert!(st.mapping.is_none());
        assert!(st.pmt.is_none());
    }

    #[test]
    fn remap_and_program_change_require_fresh_matching_pmt() {
        for mapping in [(1, 300), (2, 256)] {
            let mut st = state();
            assert!(st.observe(pat(mapping.0, mapping.1)).changed);
            assert!(st.pmt.is_none());
            assert!(st.removed.is_empty());
            st.observe(source());
            assert!(st.pmt.is_none());
        }
    }

    #[test]
    fn repeats_keep_version_and_content_changes_increment_modulo_32() {
        let mut st = state();
        let first = st.pmt.unwrap();
        assert!(!st.observe(source()).changed);
        assert_eq!(st.pmt.unwrap(), first);
        let changed = pmt(31, &[(0x1b, 257, &[]), (3, 258, &[]), (15, 260, &[])]);
        assert!(st.observe(changed).changed);
        assert_eq!((st.pmt.unwrap()[10] >> 1) & 31, 1);
        let version_only = pmt(0, &[(0x1b, 257, &[]), (3, 258, &[]), (15, 260, &[])]);
        assert!(!st.observe(version_only).changed);
        assert_eq!((st.pmt.unwrap()[10] >> 1) & 31, 1);
        let removed = pmt(1, &[(0x1b, 257, &[]), (3, 258, &[])]);
        assert!(st.observe(removed).changed);
        assert!(st.removed.is_empty());
        assert_eq!(es_pids(&st.pmt.unwrap()), vec![257, 258]);
    }

    #[test]
    fn wrong_program_or_noncurrent_fragment_cannot_disable_known_good_selection() {
        for noncurrent in [false, true] {
            let mut st = state();
            let before = st.pmt;
            let mut bad = source();
            bad[7] = 200; // unsupported fragmented length
            if noncurrent {
                bad[10] &= !1;
            } else {
                bad[9] = 2;
            }
            let result = st.observe(bad);
            assert!(!result.changed);
            assert_eq!(st.pmt, before);
            assert!(st.observe(elementary(258)).packet.is_none());
        }
    }

    #[test]
    fn single_adts_track_still_counts_injected_pmt_payloads() {
        let mut st = LiveAudioSelection::default();
        st.observe(pat(1, 256));
        let original = pmt(7, &[(0x1b, 257, &[]), (15, 259, &[])]);
        let normalized = st.observe(original).packet.unwrap();
        assert_eq!((normalized[10] >> 1) & 31, 7); // unchanged content retains source version
        let mut emitted = normalized.repeat(2);
        st.emitted(&mut emitted);
        assert_eq!(emitted[N + 3] & 15, 1);
    }

    #[test]
    fn no_audio_or_no_adts_keeps_original_table_and_es_packets() {
        for entries in [
            vec![(0x1b, 257, &[][..])],
            vec![(0x11, 258, &[][..]), (3, 259, &[][..]), (6, 260, &[][..])],
        ] {
            let mut st = LiveAudioSelection::default();
            st.observe(pat(1, 256));
            let original = pmt(3, &entries);
            assert_eq!(st.observe(original).packet, Some(original));
            for (_, pid, _) in entries {
                let input = elementary(pid);
                assert_eq!(st.observe(input).packet, Some(input));
            }
        }
    }

    #[test]
    fn pmt_adaptation_only_packets_hold_last_emitted_payload_counter() {
        let mut st = state();
        let mut payload = st.pmt.unwrap();
        st.emitted(&mut payload);
        let mut adaptation = elementary(256);
        adaptation[3] = 0x29;
        adaptation[4] = 183;
        adaptation[5] = 0;
        st.emitted(&mut adaptation);
        assert_eq!(adaptation[3] & 15, 0);
        let mut next = st.pmt.unwrap();
        st.emitted(&mut next);
        assert_eq!(next[3] & 15, 1);
    }

    #[test]
    fn emitted_prefix_and_repeated_pmt_share_payload_continuity() {
        let mut st = state();
        let p = st.pmt.unwrap();
        let mut out = p.repeat(18);
        st.emitted(&mut out);
        let cc: Vec<_> = out.chunks_exact(N).map(|p| p[3] & 15).collect();
        assert_eq!(
            cc,
            vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 0, 1]
        );
        let mut next = p;
        st.emitted(&mut next);
        assert_eq!(next[3] & 15, 2);
    }
}
