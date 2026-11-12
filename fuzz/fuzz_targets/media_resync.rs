#![no_main]
libfuzzer_sys::fuzz_target!(|data: &[u8]| ace_fuzz::media_resync(data));
