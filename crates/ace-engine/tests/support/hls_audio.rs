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

/// Two consecutive synthetic MPEG-1 Layer II frames on the declared audio PID.
/// Each frame is 417 bytes (128 kbit/s at 44.1 kHz); padding after the bounded
/// PES payload is TS stuffing and is excluded by the PES length.
pub fn mpeg_layer2_audio_packets(pid: u16) -> Vec<[u8; 188]> {
    const FRAME_BYTES: usize = 417;
    const HEADER: [u8; 4] = [0xff, 0xfd, 0x80, 0x00];
    let mut pes = vec![0; 9 + 2 * FRAME_BYTES];
    let pes_length = (pes.len() - 6) as u16;
    pes[..9].copy_from_slice(&[
        0x00,
        0x00,
        0x01,
        0xc0,
        (pes_length >> 8) as u8,
        pes_length as u8,
        0x80,
        0x00,
        0x00,
    ]);
    pes[9..13].copy_from_slice(&HEADER);
    pes[9 + FRAME_BYTES..13 + FRAME_BYTES].copy_from_slice(&HEADER);

    pes.chunks(184)
        .enumerate()
        .map(|(continuity, chunk)| {
            let mut packet = [0xff; 188];
            packet[0] = 0x47;
            packet[1] = ((pid >> 8) as u8 & 0x1f) | if continuity == 0 { 0x40 } else { 0 };
            packet[2] = pid as u8;
            packet[3] = 0x10 | (continuity as u8 & 0x0f);
            packet[4..4 + chunk.len()].copy_from_slice(chunk);
            packet
        })
        .collect()
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

/// Explicitly verified tools for the opt-in synthetic codec acceptance suite.
#[derive(Clone)]
pub struct SyntheticTools {
    pub ffmpeg: std::path::PathBuf,
    pub ffprobe: std::path::PathBuf,
}

#[derive(Clone, Copy, Debug)]
pub enum AudioCase {
    Ac3,
    MpegLayer2,
    MpegLayer3,
    AacAndAc3,
    VideoOnly,
}

pub struct SyntheticCase {
    pub transport: Vec<u8>,
    pub video_elementary: Vec<u8>,
    pub duration_seconds: f64,
}

pub fn require_synthetic_tools() -> Result<SyntheticTools, String> {
    fn executable(name: &str) -> Result<std::path::PathBuf, String> {
        let path = std::env::var_os("PATH")
            .and_then(|paths| {
                std::env::split_paths(&paths).find_map(|dir| {
                    let candidate = dir.join(name);
                    candidate.is_file().then_some(candidate)
                })
            })
            .ok_or_else(|| format!("required executable {name} is missing"))?;
        let output = std::process::Command::new(&path)
            .arg("-version")
            .output()
            .map_err(|e| format!("cannot run {}: {e}", path.display()))?;
        if !output.status.success() {
            return Err(format!("{} -version failed", path.display()));
        }
        Ok(path)
    }
    let ffmpeg = executable("ffmpeg")?;
    let ffprobe = executable("ffprobe")?;
    let encoders = std::process::Command::new(&ffmpeg)
        .args(["-hide_banner", "-encoders"])
        .output()
        .map_err(|e| format!("cannot inspect FFmpeg encoders: {e}"))?;
    let filters = std::process::Command::new(&ffmpeg)
        .args(["-hide_banner", "-filters"])
        .output()
        .map_err(|e| format!("cannot inspect FFmpeg filters: {e}"))?;
    let encoders = String::from_utf8_lossy(&encoders.stdout);
    let filters = String::from_utf8_lossy(&filters.stdout);
    for encoder in ["mpeg2video", "ac3", "mp2", "libmp3lame", "aac"] {
        if !encoders
            .lines()
            .any(|line| line.split_whitespace().any(|word| word == encoder))
        {
            return Err(format!("FFmpeg lacks required synthetic encoder {encoder}"));
        }
    }
    for filter in ["testsrc2", "sine"] {
        if !filters
            .lines()
            .any(|line| line.split_whitespace().any(|word| word == filter))
        {
            return Err(format!(
                "FFmpeg lacks required synthetic source filter {filter}"
            ));
        }
    }
    Ok(SyntheticTools { ffmpeg, ffprobe })
}

pub fn generate_case(tools: &SyntheticTools, case: AudioCase) -> Result<SyntheticCase, String> {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "outpace-hls-audio-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).map_err(|e| format!("create temp dir: {e}"))?;
    let transport_path = dir.join("source.ts");
    let video_path = dir.join("source.m2v");
    let duration = "10";
    let mut command = std::process::Command::new(&tools.ffmpeg);
    command.args([
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        "-f",
        "lavfi",
        "-i",
        "testsrc2=size=160x90:rate=25:duration=10",
    ]);
    if !matches!(case, AudioCase::VideoOnly) {
        command.args([
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=997:sample_rate=48000:duration=10",
        ]);
    }
    command.args([
        "-map",
        "0:v:0",
        "-c:v",
        "mpeg2video",
        "-g",
        "25",
        "-bf",
        "0",
    ]);
    match case {
        AudioCase::Ac3 => {
            command.args(["-map", "1:a:0", "-c:a", "ac3", "-b:a", "192k"]);
        }
        AudioCase::MpegLayer2 => {
            command.args(["-map", "1:a:0", "-c:a", "mp2", "-b:a", "192k"]);
        }
        AudioCase::MpegLayer3 => {
            command.args(["-map", "1:a:0", "-c:a", "libmp3lame", "-b:a", "128k"]);
        }
        AudioCase::AacAndAc3 => {
            command.args([
                "-map", "1:a:0", "-c:a:0", "aac", "-b:a:0", "128k", "-map", "1:a:0", "-c:a:1",
                "ac3", "-b:a:1", "192k",
            ]);
        }
        AudioCase::VideoOnly => {}
    }
    command.args(["-mpegts_flags", "+system_b"]);
    let status = command
        .arg("-t")
        .arg(duration)
        .arg("-f")
        .arg("mpegts")
        .arg(&transport_path)
        .status()
        .map_err(|e| format!("run FFmpeg synthetic source generator: {e}"))?;
    if !status.success() {
        return Err(format!(
            "FFmpeg synthetic source generation exited {status}"
        ));
    }
    let video = std::process::Command::new(&tools.ffmpeg)
        .args(["-hide_banner", "-loglevel", "error", "-y", "-i"])
        .arg(&transport_path)
        .args(["-map", "0:v:0", "-c:v", "copy", "-f", "mpeg2video"])
        .arg(&video_path)
        .status()
        .map_err(|e| format!("extract source video elementary stream: {e}"))?;
    if !video.success() {
        return Err(format!("FFmpeg source video extraction exited {video}"));
    }
    let transport =
        std::fs::read(&transport_path).map_err(|e| format!("read generated TS: {e}"))?;
    let video_elementary =
        std::fs::read(&video_path).map_err(|e| format!("read source video: {e}"))?;
    let _ = std::fs::remove_dir_all(&dir);
    Ok(SyntheticCase {
        transport,
        video_elementary,
        duration_seconds: 10.0,
    })
}
