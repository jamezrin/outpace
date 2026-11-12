#[cfg(not(feature = "srt"))]
fn main() {}

#[cfg(feature = "srt")]
fn main() {
    use std::process::Command;
    assert_eq!(
        std::env::var("CARGO_CFG_TARGET_OS").unwrap(),
        "linux",
        "SRT support currently requires Linux"
    );
    assert_eq!(
        std::env::var("HOST").unwrap(),
        std::env::var("TARGET").unwrap(),
        "SRT cross builds need a separately validated target toolchain"
    );
    let version = Command::new("pkg-config")
        .args(["--atleast-version=1.5.7", "srt"])
        .status()
        .expect("srt feature requires pkg-config and libsrt development files");
    assert!(
        version.success(),
        "srt feature requires system libsrt >= 1.5.7"
    );
    let output = |flag| {
        let out = Command::new("pkg-config")
            .args([flag, "srt"])
            .output()
            .expect("pkg-config failed");
        assert!(out.status.success(), "pkg-config failed");
        String::from_utf8(out.stdout).expect("non UTF-8 pkg-config flags")
    };
    let mut build = cc::Build::new();
    build.file("native/srt_ingest.c").warnings_into_errors(true);
    for flag in output("--cflags").split_whitespace() {
        build.flag(flag);
    }
    build.compile("outpace_srt");
    for flag in output("--libs-only-L").split_whitespace() {
        println!(
            "cargo:rustc-link-search=native={}",
            flag.strip_prefix("-L")
                .expect("unexpected library path flag")
        );
    }
    for flag in output("--libs-only-l").split_whitespace() {
        println!(
            "cargo:rustc-link-lib={}",
            flag.strip_prefix("-l").expect("unexpected library flag")
        );
    }
    println!("cargo:rerun-if-changed=native/srt_ingest.c");
}
