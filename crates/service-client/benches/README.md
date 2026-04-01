# Service Client Benchmarks

Compares the custom H2 connection pool against hyper_util's legacy client and the raw h2 client using in-memory duplex streams.

## Benchmark groups

| Group | What it measures |
|-------|-----------------|
| `sequential` | Single request latency |
| `concurrent/{10,50}` | Throughput under H2 multiplexing |
| `body-{1KB,64KB}` | Data echo throughput |

## Running benchmarks

Run all benchmarks:

```bash
cargo bench -p restate-service-client --bench h2_pool_benchmark
```

Dry-run (verify they execute without measuring):

```bash
cargo bench -p restate-service-client --bench h2_pool_benchmark -- --test
```

Run a single benchmark by name filter:

```bash
cargo bench -p restate-service-client --bench h2_pool_benchmark -- "sequential/custom-pool"
```

## CPU profiling with pprof (built-in)

The benchmarks include [pprof](https://github.com/tikv/pprof-rs) integration that generates flamegraph SVGs.

### Prerequisites

On Linux, allow perf events:

```bash
sudo sysctl kernel.perf_event_paranoid=-1
```

### Profile a benchmark

Pass `--profile-time=<seconds>` to activate profiling:

```bash
cargo bench -p restate-service-client --bench h2_pool_benchmark -- "sequential/custom-pool" --profile-time=30
```

The flamegraph is written to:

```
target/criterion/sequential/custom-pool/profile/flamegraph.svg
```

Open it in a browser for an interactive view.

## CPU profiling with samply (external)

[samply](https://github.com/mstange/samply) can profile the benchmark binary without any code changes.

```bash
# Build the benchmark binary (release mode)
cargo bench -p restate-service-client --bench h2_pool_benchmark --no-run

# Find and run with samply
samply record target/release/deps/h2_pool_benchmark-* --bench "sequential/custom-pool" --profile-time=30
```

This opens the Firefox Profiler UI automatically.

# Benchmarks results
Result of running the benchmarks on my local machine

```
    Finished `bench` profile [optimized + debuginfo] target(s) in 0.39s
     Running benches/h2_pool_benchmark.rs (target/release/deps/h2_pool_benchmark-1c5a1d412e3d2788)
sequential/custom-pool  time:   [10.749 µs 10.860 µs 11.006 µs]
                        change: [-0.7158% +1.0153% +2.9501%] (p = 0.30 > 0.05)
                        No change in performance detected.
Found 4 outliers among 50 measurements (8.00%)
  1 (2.00%) high mild
  3 (6.00%) high severe
sequential/hyper-util-legacy
                        time:   [14.488 µs 14.667 µs 14.858 µs]
                        change: [-13.068% -8.1283% -3.6317%] (p = 0.00 < 0.05)
                        Performance has improved.
Found 1 outliers among 50 measurements (2.00%)
  1 (2.00%) high severe
sequential/h2-raw       time:   [10.664 µs 10.775 µs 10.907 µs]
                        change: [+0.4993% +1.7943% +3.3501%] (p = 0.01 < 0.05)
                        Change within noise threshold.
Found 3 outliers among 50 measurements (6.00%)
  2 (4.00%) high mild
  1 (2.00%) high severe

concurrent/custom-pool/10
                        time:   [97.105 µs 98.149 µs 99.240 µs]
                        thrpt:  [100.77 Kelem/s 101.89 Kelem/s 102.98 Kelem/s]
                 change:
                        time:   [-5.2687% -2.6071% -0.0896%] (p = 0.06 > 0.05)
                        thrpt:  [+0.0897% +2.6769% +5.5617%]
                        No change in performance detected.
Found 1 outliers among 30 measurements (3.33%)
  1 (3.33%) high mild
concurrent/hyper-util-legacy/10
                        time:   [85.443 µs 86.182 µs 87.059 µs]
                        thrpt:  [114.87 Kelem/s 116.03 Kelem/s 117.04 Kelem/s]
                 change:
                        time:   [-1.1895% -0.0168% +1.2324%] (p = 0.98 > 0.05)
                        thrpt:  [-1.2174% +0.0168% +1.2038%]
                        No change in performance detected.
Found 1 outliers among 30 measurements (3.33%)
  1 (3.33%) high mild
concurrent/h2-raw/10    time:   [93.670 µs 94.724 µs 95.684 µs]
                        thrpt:  [104.51 Kelem/s 105.57 Kelem/s 106.76 Kelem/s]
                 change:
                        time:   [-2.7715% -1.4009% -0.0251%] (p = 0.07 > 0.05)
                        thrpt:  [+0.0251% +1.4208% +2.8505%]
                        No change in performance detected.

concurrent/custom-pool/50
                        time:   [673.80 µs 705.73 µs 739.56 µs]
                        thrpt:  [67.607 Kelem/s 70.849 Kelem/s 74.206 Kelem/s]
                 change:
                        time:   [-2.2779% +3.2861% +9.5124%] (p = 0.28 > 0.05)
                        thrpt:  [-8.6861% -3.1815% +2.3310%]
                        No change in performance detected.
concurrent/hyper-util-legacy/50
                        time:   [463.46 µs 467.62 µs 472.13 µs]
                        thrpt:  [105.90 Kelem/s 106.93 Kelem/s 107.88 Kelem/s]
                 change:
                        time:   [-0.6146% +0.3699% +1.4191%] (p = 0.50 > 0.05)
                        thrpt:  [-1.3993% -0.3685% +0.6184%]
                        No change in performance detected.
concurrent/h2-raw/50    time:   [541.15 µs 547.47 µs 553.99 µs]
                        thrpt:  [90.254 Kelem/s 91.330 Kelem/s 92.397 Kelem/s]
                 change:
                        time:   [-4.1143% -2.5260% -1.0413%] (p = 0.00 < 0.05)
                        thrpt:  [+1.0523% +2.5915% +4.2908%]
                        Performance has improved.

body-1KB/custom-pool    time:   [15.059 µs 15.195 µs 15.353 µs]
                        thrpt:  [63.606 MiB/s 64.267 MiB/s 64.851 MiB/s]
                 change:
                        time:   [-8.9677% -6.9609% -4.9485%] (p = 0.00 < 0.05)
                        thrpt:  [+5.2061% +7.4816% +9.8512%]
                        Performance has improved.
Found 1 outliers among 30 measurements (3.33%)
  1 (3.33%) high severe
body-1KB/hyper-util-legacy
                        time:   [17.867 µs 18.117 µs 18.407 µs]
                        thrpt:  [53.053 MiB/s 53.903 MiB/s 54.658 MiB/s]
                 change:
                        time:   [-3.6507% +0.4844% +4.3818%] (p = 0.82 > 0.05)
                        thrpt:  [-4.1978% -0.4820% +3.7890%]
                        No change in performance detected.
Found 6 outliers among 30 measurements (20.00%)
  3 (10.00%) high mild
  3 (10.00%) high severe
body-1KB/h2-raw         time:   [14.847 µs 14.935 µs 15.025 µs]
                        thrpt:  [64.994 MiB/s 65.388 MiB/s 65.773 MiB/s]
                 change:
                        time:   [-1.5745% -0.5824% +0.3154%] (p = 0.26 > 0.05)
                        thrpt:  [-0.3144% +0.5859% +1.5997%]
                        No change in performance detected.
Found 1 outliers among 30 measurements (3.33%)
  1 (3.33%) high mild

body-64KB/custom-pool   time:   [47.503 µs 48.331 µs 49.510 µs]
                        thrpt:  [1.2328 GiB/s 1.2628 GiB/s 1.2849 GiB/s]
                 change:
                        time:   [-2.8939% -1.4606% -0.0404%] (p = 0.06 > 0.05)
                        thrpt:  [+0.0404% +1.4822% +2.9801%]
                        No change in performance detected.
Found 5 outliers among 30 measurements (16.67%)
  3 (10.00%) high mild
  2 (6.67%) high severe
body-64KB/hyper-util-legacy
                        time:   [50.120 µs 56.136 µs 62.801 µs]
                        thrpt:  [995.21 MiB/s 1.0873 GiB/s 1.2178 GiB/s]
                 change:
                        time:   [-5.0280% +2.9743% +11.921%] (p = 0.51 > 0.05)
                        thrpt:  [-10.652% -2.8884% +5.2942%]
                        No change in performance detected.
Found 2 outliers among 30 measurements (6.67%)
  2 (6.67%) high mild
body-64KB/h2-raw        time:   [48.884 µs 50.233 µs 51.896 µs]
                        thrpt:  [1.1761 GiB/s 1.2151 GiB/s 1.2486 GiB/s]
                 change:
                        time:   [-0.6970% +1.9538% +4.7254%] (p = 0.16 > 0.05)
                        thrpt:  [-4.5121% -1.9164% +0.7018%]
                        No change in performance detected.
Found 2 outliers among 30 measurements (6.67%)
  1 (3.33%) high mild
  1 (3.33%) high severe
```