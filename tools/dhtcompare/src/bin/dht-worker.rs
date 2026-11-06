//! Offline research entry: arguments alone never authorize network access.
fn main() {
    let status = outpace_dht_compare::worker::entry(
        std::env::args_os().skip(1),
        std::env::vars_os,
        &mut std::io::stdout().lock(),
    );
    std::process::exit(status);
}
