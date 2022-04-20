use criterion::{criterion_group, criterion_main, Criterion};

use tempfile::TempDir;
use std::fs::File;
use libflatterer::{flatten, Options};
use std::io::BufReader;

fn weather_test() {
    let tmp_dir = TempDir::new().unwrap();
    let output_dir = tmp_dir.path().join("output");
    let options = Options::builder().json_stream(true).build();
    
    flatten(
        BufReader::new(File::open("fixtures/daily_16.json").unwrap()), // reader
        output_dir.to_string_lossy().into(), // output directory
        options, // FlatFile instance.
    ).unwrap(); // Path to array.
}

pub fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("weather", |b| b.iter(|| weather_test()));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);