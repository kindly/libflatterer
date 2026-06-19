use std::fs::File;
use std::io::BufReader;
use std::time::Instant;

use libflatterer::{flatten, Options};

// Usage: cargo run --release --example bench -- <input> <output_dir> [ndjson|json_stream|array] [threads]
fn main() {
    let args: Vec<String> = std::env::args().collect();
    let input = args.get(1).expect("input path");
    let output = args.get(2).expect("output dir").clone();
    let mode = args.get(3).map(|s| s.as_str()).unwrap_or("array");
    let threads: usize = args
        .get(4)
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);

    // Attribution modes: just read lines and parse, no flattening/writing.
    // parseonly -> serde_json::Value, fastparse -> libflatterer::fastvalue::FastValue.
    if mode == "parseonly" || mode == "fastparse" {
        use std::io::BufRead;
        let reader = BufReader::new(File::open(input).expect("open input"));
        let start = Instant::now();
        let mut n = 0usize;
        for line in reader.lines() {
            let line = line.unwrap();
            if line.trim().is_empty() {
                continue;
            }
            if mode == "fastparse" {
                let _v: libflatterer::fastvalue::FastValue =
                    serde_json::from_str(&line).unwrap();
            } else {
                let _v: serde_json::Value = serde_json::from_str(&line).unwrap();
            }
            n += 1;
        }
        println!(
            "mode={mode} lines={n} elapsed={:.3}s",
            start.elapsed().as_secs_f64()
        );
        return;
    }

    // Attribution mode: drive the single-thread worker loop manually (ndjson) and
    // time parse vs process_value (handle_obj) vs create_rows (csv build/write) separately.
    if mode == "attrib" {
        use std::io::BufRead;
        use std::time::Duration;
        let _ = std::fs::remove_dir_all(&output);
        let mut opts = Options::builder().force(true).threads(1).build();
        opts.ndjson = true;
        let no_link = args.get(4).map(|s| s == "nolink").unwrap_or(false);
        opts.no_link = no_link;
        let mut ff = libflatterer::FlatFiles::new(output.clone(), opts).expect("new FlatFiles");
        let reader = BufReader::new(File::open(input).expect("open input"));
        let (mut t_parse, mut t_proc, mut t_rows) =
            (Duration::ZERO, Duration::ZERO, Duration::ZERO);
        let mut n = 0usize;
        for line in reader.lines() {
            let line = line.unwrap();
            if line.trim().is_empty() {
                continue;
            }
            let s = Instant::now();
            let value: libflatterer::fastvalue::FastValue = serde_json::from_str(&line).unwrap();
            t_parse += s.elapsed();
            let s = Instant::now();
            ff.process_fast_value(value, Vec::new());
            t_proc += s.elapsed();
            let s = Instant::now();
            ff.create_rows().unwrap();
            t_rows += s.elapsed();
            n += 1;
        }
        let s = Instant::now();
        ff.write_files().unwrap();
        let t_write = s.elapsed();
        let total = (t_parse + t_proc + t_rows + t_write).as_secs_f64();
        println!(
            "attrib(no_link={no_link}) lines={n} total={total:.3}s | parse={:.3}s ({:.0}%) process_value={:.3}s ({:.0}%) create_rows={:.3}s ({:.0}%) write_files={:.3}s ({:.0}%)",
            t_parse.as_secs_f64(), 100.0 * t_parse.as_secs_f64() / total,
            t_proc.as_secs_f64(), 100.0 * t_proc.as_secs_f64() / total,
            t_rows.as_secs_f64(), 100.0 * t_rows.as_secs_f64() / total,
            t_write.as_secs_f64(), 100.0 * t_write.as_secs_f64() / total,
        );
        return;
    }

    let _ = std::fs::remove_dir_all(&output);

    let mut options = Options::builder().force(true).threads(threads).build();
    match mode {
        "ndjson" => options.ndjson = true,
        "json_stream" => options.json_stream = true,
        "array" => {}
        other => panic!("unknown mode {other}"),
    }
    // FLATTEN_PATH=a,b sets --path.
    if let Ok(p) = std::env::var("FLATTEN_PATH") {
        if !p.is_empty() {
            options.path = p.split(',').map(String::from).collect();
        }
    }

    let reader = BufReader::new(File::open(input).expect("open input"));

    let start = Instant::now();
    flatten(reader, output, options).expect("flatten failed");
    let elapsed = start.elapsed();
    println!("mode={mode} threads={threads} elapsed={:.3}s", elapsed.as_secs_f64());
}
