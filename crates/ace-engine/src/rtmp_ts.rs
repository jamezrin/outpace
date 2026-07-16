//! Narrow RTMP H.264/AAC to MPEG-TS remuxing for broadcast ingest.

use rtmp_rs::media::aac::{generate_adts_header, AudioSpecificConfig};
use rtmp_rs::media::h264::{AvcConfig, NaluIterator};
use rtmp_rs::media::{AacData, H264Data};

const PAT_PID: u16 = 0x0000;
const PMT_PID: u16 = 0x1000;
const VIDEO_PID: u16 = 0x0100;
const AUDIO_PID: u16 = 0x0101;
const VIDEO_STREAM_ID: u8 = 0xe0;
const AUDIO_STREAM_ID: u8 = 0xc0;

pub struct RtmpTsMuxer {
    video_config: Option<AvcConfig>,
    audio_config: Option<AudioSpecificConfig>,
    pat_cc: u8,
    pmt_cc: u8,
    video_cc: u8,
    audio_cc: u8,
    wrote_tables: bool,
}

impl Default for RtmpTsMuxer {
    fn default() -> Self {
        Self::new()
    }
}

impl RtmpTsMuxer {
    pub fn new() -> Self {
        Self {
            video_config: None,
            audio_config: None,
            pat_cc: 0,
            pmt_cc: 0,
            video_cc: 0,
            audio_cc: 0,
            wrote_tables: false,
        }
    }

    pub fn push_video(&mut self, frame: &H264Data, timestamp_ms: u32) -> Vec<u8> {
        match frame {
            H264Data::SequenceHeader(config) => {
                self.video_config = Some(config.clone());
                Vec::new()
            }
            H264Data::Frame {
                keyframe,
                composition_time,
                nalus,
            } => {
                let Some(config) = self.video_config.clone() else {
                    return Vec::new();
                };
                let mut access_unit = Vec::new();
                access_unit.extend_from_slice(&[0x00, 0x00, 0x00, 0x01, 0x09, 0xf0]);
                if *keyframe {
                    for sps in &config.sps {
                        push_annex_b_nalu(&mut access_unit, sps);
                    }
                    for pps in &config.pps {
                        push_annex_b_nalu(&mut access_unit, pps);
                    }
                }
                for nalu in NaluIterator::new(nalus.as_ref(), config.nalu_length_size) {
                    push_annex_b_nalu(&mut access_unit, nalu);
                }
                let dts = pts90(timestamp_ms);
                let pts_ms = timestamp_ms.saturating_add((*composition_time).max(0) as u32);
                let pts = pts90(pts_ms);
                self.packetize_pes(VIDEO_PID, VIDEO_STREAM_ID, dts, Some(pts), &access_unit)
            }
            H264Data::EndOfSequence => Vec::new(),
        }
    }

    pub fn push_audio(&mut self, frame: &AacData, timestamp_ms: u32) -> Vec<u8> {
        match frame {
            AacData::SequenceHeader(config) => {
                self.audio_config = Some(config.clone());
                Vec::new()
            }
            AacData::Frame { data } => {
                let Some(config) = self.audio_config.clone() else {
                    return Vec::new();
                };
                let mut aac = Vec::with_capacity(7 + data.len());
                aac.extend_from_slice(&generate_adts_header(&config, data.len()));
                aac.extend_from_slice(data);
                let pts = pts90(timestamp_ms);
                self.packetize_pes(AUDIO_PID, AUDIO_STREAM_ID, pts, None, &aac)
            }
        }
    }

    fn packetize_pes(
        &mut self,
        pid: u16,
        stream_id: u8,
        dts: u64,
        pts: Option<u64>,
        payload: &[u8],
    ) -> Vec<u8> {
        let mut out = Vec::new();
        if !self.wrote_tables {
            self.write_tables(&mut out);
            self.wrote_tables = true;
        }
        let pes = pes_packet(stream_id, dts, pts, payload);
        let cc = if pid == VIDEO_PID {
            &mut self.video_cc
        } else {
            &mut self.audio_cc
        };
        write_ts_payload(&mut out, pid, cc, true, &pes);
        out
    }

    fn write_tables(&mut self, out: &mut Vec<u8>) {
        let pat = pat_section();
        let pmt = pmt_section();
        write_ts_payload(out, PAT_PID, &mut self.pat_cc, true, &pat);
        write_ts_payload(out, PMT_PID, &mut self.pmt_cc, true, &pmt);
    }
}

fn push_annex_b_nalu(out: &mut Vec<u8>, nalu: &[u8]) {
    out.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]);
    out.extend_from_slice(nalu);
}

fn pts90(ms: u32) -> u64 {
    u64::from(ms) * 90
}

fn pes_packet(stream_id: u8, dts: u64, pts: Option<u64>, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(19 + payload.len());
    out.extend_from_slice(&[0x00, 0x00, 0x01, stream_id]);
    out.extend_from_slice(&[0x00, 0x00]);
    out.push(0x80);
    match pts {
        Some(pts) if pts != dts => {
            out.push(0xc0);
            out.push(10);
            write_ts_timestamp(&mut out, 0x03, pts);
            write_ts_timestamp(&mut out, 0x01, dts);
        }
        Some(pts) => {
            out.push(0x80);
            out.push(5);
            write_ts_timestamp(&mut out, 0x02, pts);
        }
        None => {
            out.push(0x80);
            out.push(5);
            write_ts_timestamp(&mut out, 0x02, dts);
        }
    }
    out.extend_from_slice(payload);
    out
}

fn write_ts_timestamp(out: &mut Vec<u8>, prefix: u8, ts: u64) {
    let ts = ts & 0x1ffffffff;
    out.push((prefix << 4) | (((ts >> 30) as u8 & 0x07) << 1) | 1);
    out.push((ts >> 22) as u8);
    out.push((((ts >> 15) as u8 & 0x7f) << 1) | 1);
    out.push((ts >> 7) as u8);
    out.push(((ts as u8 & 0x7f) << 1) | 1);
}

fn write_ts_payload(out: &mut Vec<u8>, pid: u16, cc: &mut u8, start: bool, payload: &[u8]) {
    let mut rest = payload;
    let mut first = true;
    while !rest.is_empty() {
        let payload_start = first && start;
        let payload_capacity = 184;
        let take = rest.len().min(payload_capacity);
        let remaining_space = payload_capacity - take;
        let adaptation = remaining_space > 0;

        out.push(0x47);
        out.push(((payload_start as u8) << 6) | ((pid >> 8) as u8 & 0x1f));
        out.push(pid as u8);
        out.push(if adaptation { 0x30 } else { 0x10 } | (*cc & 0x0f));
        *cc = (*cc + 1) & 0x0f;

        if adaptation {
            out.push(remaining_space as u8 - 1);
            if remaining_space > 1 {
                out.push(0x00);
                out.extend(std::iter::repeat_n(0xff, remaining_space - 2));
            }
        }

        out.extend_from_slice(&rest[..take]);
        rest = &rest[take..];
        first = false;
    }
}

fn pat_section() -> Vec<u8> {
    let mut section = vec![
        0x00,
        0x00,
        0xb0,
        0x0d,
        0x00,
        0x01,
        0xc1,
        0x00,
        0x00,
        0x00,
        0x01,
        0xf0 | ((PMT_PID >> 8) as u8 & 0x1f),
        PMT_PID as u8,
    ];
    let crc = ace_media::mpegts::mpeg_crc32(&section[1..]);
    section.extend_from_slice(&crc.to_be_bytes());
    section
}

fn pmt_section() -> Vec<u8> {
    let section_len = 9 + 5 + 5 + 4;
    let mut section = vec![
        0x00,
        0x02,
        0xb0 | ((section_len >> 8) as u8 & 0x0f),
        section_len as u8,
        0x00,
        0x01,
        0xc1,
        0x00,
        0x00,
        0xe0 | ((VIDEO_PID >> 8) as u8 & 0x1f),
        VIDEO_PID as u8,
        0xf0,
        0x00,
        0x1b,
        0xe0 | ((VIDEO_PID >> 8) as u8 & 0x1f),
        VIDEO_PID as u8,
        0xf0,
        0x00,
        0x0f,
        0xe0 | ((AUDIO_PID >> 8) as u8 & 0x1f),
        AUDIO_PID as u8,
        0xf0,
        0x00,
    ];
    let crc = ace_media::mpegts::mpeg_crc32(&section[1..]);
    section.extend_from_slice(&crc.to_be_bytes());
    section
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use rtmp_rs::media::aac::{generate_adts_header, AudioSpecificConfig};
    use rtmp_rs::media::{AacData, H264Data};

    fn avc_config() -> rtmp_rs::media::h264::AvcConfig {
        rtmp_rs::media::h264::AvcConfig {
            profile: 100,
            compatibility: 0,
            level: 31,
            nalu_length_size: 4,
            sps: vec![Bytes::from_static(&[0x67, 0x64, 0x00, 0x1f])],
            pps: vec![Bytes::from_static(&[0x68, 0xef, 0x38])],
            raw: Bytes::new(),
        }
    }

    fn aac_config() -> AudioSpecificConfig {
        AudioSpecificConfig::parse(Bytes::from_static(&[0x12, 0x10])).unwrap()
    }

    #[test]
    fn muxer_emits_aligned_ts_for_h264_and_aac() {
        let mut muxer = RtmpTsMuxer::new();
        let mut out = Vec::new();

        out.extend(muxer.push_video(&H264Data::SequenceHeader(avc_config()), 0));
        out.extend(muxer.push_audio(&AacData::SequenceHeader(aac_config()), 0));
        out.extend(muxer.push_video(
            &H264Data::Frame {
                keyframe: true,
                composition_time: 0,
                nalus: Bytes::from_static(&[0x00, 0x00, 0x00, 0x05, 0x65, 0x88, 0x84, 0x21, 0xa0]),
            },
            40,
        ));
        out.extend(muxer.push_audio(
            &AacData::Frame {
                data: Bytes::from_static(&[0x21, 0x10, 0x56, 0xe5]),
            },
            42,
        ));

        assert!(!out.is_empty());
        assert_eq!(out.len() % ace_media::mpegts::TS_PACKET_LEN, 0);
        assert!(out.chunks(188).all(|packet| packet[0] == 0x47));

        let adts = generate_adts_header(&aac_config(), 4);
        assert!(
            out.windows(adts.len()).any(|window| window == adts),
            "AAC frames must be wrapped in ADTS before MPEG-TS muxing"
        );
        assert!(
            out.windows(4)
                .any(|window| window == [0x00, 0x00, 0x00, 0x01]),
            "H.264 frames must be converted to Annex B start-code form"
        );
    }

    #[test]
    fn psi_section_lengths_match_encoded_payloads() {
        for section in [pat_section(), pmt_section()] {
            let section_length = (usize::from(section[2] & 0x0f) << 8) | usize::from(section[3]);
            assert_eq!(
                section_length,
                section.len() - 4,
                "PSI section_length must cover bytes after length field through CRC"
            );
        }
    }
}
