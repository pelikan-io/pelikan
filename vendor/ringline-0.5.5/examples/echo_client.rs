use std::io::{self, BufRead, Read, Write};
use std::net::TcpStream;

fn main() {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:7878".to_string());

    let mut stream = TcpStream::connect(&addr).expect("failed to connect");
    eprintln!("connected to {addr}");

    let stdin = io::stdin();
    for line in stdin.lock().lines() {
        let line = line.expect("failed to read stdin");
        stream.write_all(line.as_bytes()).expect("failed to send");
        stream.write_all(b"\n").expect("failed to send newline");

        let mut buf = vec![0u8; line.len() + 1];
        stream
            .read_exact(&mut buf)
            .expect("failed to read response");
        print!("{}", String::from_utf8_lossy(&buf));
    }
}
