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
    for name in [
        "PKG_CONFIG",
        "PKG_CONFIG_PATH",
        "PKG_CONFIG_LIBDIR",
        "PKG_CONFIG_SYSROOT_DIR",
        "PKG_CONFIG_DISABLE_UNINSTALLED",
        "PKG_CONFIG_ALLOW_SYSTEM_CFLAGS",
        "PKG_CONFIG_ALLOW_SYSTEM_LIBS",
    ] {
        println!("cargo:rerun-if-env-changed={name}");
    }
    let executable = std::env::var_os("PKG_CONFIG").unwrap_or_else(|| "pkg-config".into());
    let version = Command::new(&executable)
        .args(["--atleast-version=1.5.7", "srt"])
        .status()
        .expect("srt feature requires pkg-config and libsrt development files");
    assert!(
        version.success(),
        "srt feature requires system libsrt >= 1.5.7"
    );
    let output = |flag| {
        let out = Command::new(&executable)
            .args([flag, "srt"])
            .output()
            .expect("pkg-config failed");
        assert!(out.status.success(), "pkg-config failed");
        shlex::split(&String::from_utf8(out.stdout).expect("non UTF-8 pkg-config flags"))
            .expect("invalid pkg-config flag escaping")
    };
    let mut build = cc::Build::new();
    build.file("native/srt_ingest.c").warnings_into_errors(true);
    for flag in &output("--cflags") {
        build.flag(flag);
    }
    build.compile("outpace_srt");
    for flag in &output("--libs-only-L") {
        println!(
            "cargo:rustc-link-search=native={}",
            flag.strip_prefix("-L")
                .expect("unexpected library path flag")
        );
    }
    for flag in &output("--libs-only-l") {
        println!(
            "cargo:rustc-link-lib={}",
            flag.strip_prefix("-l").expect("unexpected library flag")
        );
    }
    println!("cargo:rerun-if-changed=native/srt_ingest.c");
}
