// To print the build date and time from within a Rust binary,
// you can use a build.rs script to set an environment variable with the build date and time,
// and then access this variable in your main code.
use std::process::Command;

fn main() {
    let output = Command::new("date")
        .arg("+%Y-%m-%d %H:%M:%S")
        .output()
        .expect("Failed to execute command");

    let build_time = String::from_utf8(output.stdout).expect("Invalid UTF-8 sequence");
    println!("cargo:rustc-env=BUILD_TIME={}", build_time.trim());
}
