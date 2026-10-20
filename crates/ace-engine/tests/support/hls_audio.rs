//! Helpers shared by the HLS audio fault integration tests.

#[path = "hls_audio_child.rs"]
pub mod child;

/// Valid MPEG-TS framing fixture with sync bytes at each packet boundary. The
/// This is valid synthetic PSI, PCR and access point data supplied by the fixture.
/// The test factory strips only the fixed libtest startup prefix; production
/// validation receives these raw transport packets.
pub fn transport_prefix() -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&psi_packet(0, 0, &pat_section(7, 9, 4096)));
    bytes.extend_from_slice(&psi_packet(
        4096,
        2,
        &pmt_section(9, 256, 256, 0x1b, 257, 0x0f),
    ));
    bytes.extend_from_slice(&video_access_packet(256));
    bytes
}

pub fn pat_section(tsid: u16, program: u16, pmt_pid: u16) -> Vec<u8> {
    let mut s = vec![0x00, 0xb0, 0x0d];
    s.extend_from_slice(&tsid.to_be_bytes());
    s.extend_from_slice(&[0xc1, 0, 0]);
    s.extend_from_slice(&program.to_be_bytes());
    s.extend_from_slice(&[0xe0 | (pmt_pid >> 8) as u8 & 0x1f, pmt_pid as u8]);
    finish_section(s)
}

pub fn pmt_section(
    program: u16,
    pcr: u16,
    video: u16,
    video_type: u8,
    audio: u16,
    audio_type: u8,
) -> Vec<u8> {
    let mut s = vec![0x02, 0xb0, 0x17];
    s.extend_from_slice(&program.to_be_bytes());
    s.extend_from_slice(&[
        0xc1,
        0,
        0,
        0xe0 | (pcr >> 8) as u8 & 0x1f,
        pcr as u8,
        0xf0,
        0,
    ]);
    for (ty, pid) in [(video_type, video), (audio_type, audio)] {
        s.extend_from_slice(&[ty, 0xe0 | (pid >> 8) as u8 & 0x1f, pid as u8, 0xf0, 0]);
    }
    finish_section(s)
}

fn finish_section(mut s: Vec<u8>) -> Vec<u8> {
    let crc = ace_media::mpegts::mpeg_crc32(&s);
    s.extend_from_slice(&crc.to_be_bytes());
    s
}

pub fn psi_packet(pid: u16, table: u8, section: &[u8]) -> [u8; 188] {
    let mut p = [0xff; 188];
    p[..5].copy_from_slice(&[0x47, 0x40 | ((pid >> 8) as u8 & 0x1f), pid as u8, 0x10, 0]);
    assert_eq!(section[0], table);
    p[5..5 + section.len()].copy_from_slice(section);
    p
}

pub fn pat_packet_with_section(section: Vec<u8>) -> [u8; 188] {
    let section = finish_section(section);
    psi_packet(0, 0, &section)
}

pub fn pmt_packet_with_section(mut section: Vec<u8>) -> [u8; 188] {
    let length = section.len() - 3 + 4;
    section[1] = 0xb0 | ((length >> 8) as u8 & 0x0f);
    section[2] = length as u8;
    psi_packet(4096, 2, &finish_section(section))
}

pub fn video_access_packet(pid: u16) -> [u8; 188] {
    let mut p = [0xff; 188];
    p[..6].copy_from_slice(&[
        0x47,
        0x40 | ((pid >> 8) as u8 & 0x1f),
        pid as u8,
        0x30,
        1,
        0x40,
    ]);
    p
}

pub fn pcr_packet(pid: u16, pcr: u64) -> [u8; 188] {
    let mut p = [0xff; 188];
    p[0] = 0x47;
    p[1] = 0x40 | ((pid >> 8) as u8 & 0x1f);
    p[2] = pid as u8;
    p[3] = 0x30;
    p[4] = 7;
    p[5] = 0x50; // random_access_indicator and PCR_flag
    p[6] = (pcr >> 25) as u8;
    p[7] = (pcr >> 17) as u8;
    p[8] = (pcr >> 9) as u8;
    p[9] = (pcr >> 1) as u8;
    p[10] = ((pcr & 1) << 7) as u8 | 0x7e;
    p[11] = 0;
    p
}

pub fn ready_output_fixture(video_packets: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity((video_packets + 2) * 188);
    out.extend_from_slice(&psi_packet(0, 0, &pat_section(7, 9, 4096)));
    out.extend_from_slice(&psi_packet(
        4096,
        2,
        &pmt_section(9, 256, 256, 0x1b, 257, 0x0f),
    ));
    for i in 0..video_packets {
        out.extend_from_slice(&pcr_packet(256, i as u64 * 90_000));
    }
    out
}

pub fn malformed_short_pcr_packet(pid: u16) -> [u8; 188] {
    let mut p = [0xff; 188];
    p[0] = 0x47;
    p[1] = 0x40 | ((pid >> 8) as u8 & 0x1f);
    p[2] = pid as u8;
    p[3] = 0x30;
    p[4] = 6;
    p[5] = 0x50; // PCR_flag set, but adaptation field is too short for PCR bytes.
    p
}

pub fn set_psi_crc(packet: &mut [u8; 188]) {
    let start = 5 + packet[4] as usize;
    let len = 3 + (((packet[start + 1] as usize & 0x0f) << 8) | packet[start + 2] as usize);
    let crc_at = start + len - 4;
    let crc = ace_media::mpegts::mpeg_crc32(&packet[start..crc_at]);
    packet[crc_at..crc_at + 4].copy_from_slice(&crc.to_be_bytes());
}
