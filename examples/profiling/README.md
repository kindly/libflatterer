# Profiling libflatterer

Reproducible speed measurements for the flatten hot path.

## Files

- `../bench.rs` — the benchmark binary (a cargo example). Flattens an input file and
  prints `elapsed=<seconds>`. Run directly with:
  ```
  cargo run --release --example bench -- <input> <output_dir> [ndjson|json_stream|array|parseonly] [threads]
  ```
  Modes:
  - `ndjson` — input is JSON-lines (one object per line); fastest, parallelises well.
  - `array` — input is a single JSON array of objects (uses the YAJL streaming front-end).
  - `json_stream` — input is concatenated JSON objects.
  - `parseonly` — just `serde_json`-parses each line, no flatten/write (attribution).
- `profile.py` — orchestrates the measurements (build + run + min-of-N).
- `data/` — git-ignored (kept out of the repo to stay light). Place a JSON-lines file at
  `data/digi.json` (e.g. 7619 lines of OCDS data) to run the harness as-is.
- `data/digi_array.json` — the array variant; `profile.py` generates it from `digi.json` if missing.

## Run

```
python3 examples/profiling/profile.py            # default: 5 repeats, threads 1,2,4,8
python3 examples/profiling/profile.py --repeats 8 --threads 1,4
```

## Measurement note

This machine uses the `powersave` CPU governor and often has a browser using CPU,
so single-thread wall-clock varies ~2x run-to-run. The script reports the **min**
of N interleaved runs, which approximates the contention-free time. Trust the min,
never a single run.

## Baseline (libflatterer 0.24.1, contention-free)

- ndjson, 1 thread: ~0.68s; of that serde parse ~0.166s (~24%), flatten+write ~76%.
- ndjson scales to ~0.22s at 8 threads.
- array input plateaus ~0.41s regardless of threads — the YAJL front-end that
  re-serialises each object back to a string (then re-parses it) is single-threaded.
