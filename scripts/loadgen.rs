//! Minimal HTTP load generator for nonodb, over the Unix socket.
//!
//! Python clients are GIL-bound: one `httpx` request costs ~0.6 ms of client
//! CPU, which caps a whole benchmark at ~1.5k requests/s regardless of what the
//! daemon does. This tool measures the server: raw HTTP/1.1 on a kept-alive
//! socket, one thread per connection, no dependencies.
//!
//! ```bash
//! rustc -O scripts/loadgen.rs -o /tmp/loadgen
//! NONODB_DATA=/var/tmp/bench NONODB_SOCK=/var/tmp/bench/nonodb.sock \
//!     target/release/nonodb-daemon &
//! /tmp/loadgen /var/tmp/bench/nonodb.sock 16 3000        # 16 conns x 3000 inserts
//! /tmp/loadgen /var/tmp/bench/nonodb.sock 16 3000 100    # 100-byte padding
//! /tmp/loadgen /var/tmp/bench/nonodb.sock 16 3000 100 relaxed
//! ```
//!
//! It creates the `plain` collection if it does not exist. Compare the result
//! against `streams_bench.py` on the same host: the gap is the client.

use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

fn one_request(sock: &mut UnixStream, body: &str) -> usize {
    let request = format!(
        "POST /v1/exec HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
        body.len(),
        body
    );
    sock.write_all(request.as_bytes()).expect("write request");
    let mut reader = BufReader::new(sock.try_clone().expect("clone socket"));
    let mut line = String::new();
    let mut length = 0usize;
    loop {
        line.clear();
        if reader.read_line(&mut line).expect("read header") == 0 {
            panic!("connection closed by the daemon");
        }
        let trimmed = line.trim_end();
        if trimmed.is_empty() {
            break;
        }
        if let Some(value) = trimmed.to_ascii_lowercase().strip_prefix("content-length:") {
            length = value.trim().parse().unwrap_or(0);
        }
    }
    let mut body = vec![0u8; length];
    reader.read_exact(&mut body).expect("read body");
    length
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 4 {
        eprintln!(
            "usage: loadgen <socket> <threads> <requests-per-thread> [padding-bytes] [durability]"
        );
        std::process::exit(2);
    }
    let socket = args[1].clone();
    let threads: usize = args[2].parse().expect("threads");
    let per_thread: usize = args[3].parse().expect("requests");
    let padding: usize = args.get(4).map(|v| v.parse().expect("padding")).unwrap_or(100);
    let durability = args.get(5).cloned().unwrap_or_else(|| "full".into());

    let durability = Arc::new(durability);
    let done = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    let mut handles = Vec::new();
    for thread in 0..threads {
        let socket = socket.clone();
        let done = done.clone();
        let durability = durability.clone();
        handles.push(std::thread::spawn(move || {
            let mut stream = UnixStream::connect(&socket).expect("connect");
            let body = format!(
                "{{\"op\":\"insert\",\"coll\":\"plain\",\"doc\":{{\"i\":{thread},\"pad\":\"{}\"}},\"durability\":\"{durability}\"}}",
                "x".repeat(padding)
            );
            for _ in 0..per_thread {
                one_request(&mut stream, &body);
                done.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    for handle in handles {
        handle.join().expect("client thread");
    }
    let elapsed = start.elapsed().as_secs_f64();
    let requests = done.load(Ordering::Relaxed) as f64;
    println!(
        "{threads} connections x {per_thread}: {:.0} requests/s ({:.3} ms/request)",
        requests / elapsed,
        elapsed * 1000.0 / requests
    );
}
