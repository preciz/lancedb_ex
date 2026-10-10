# Benchmarks

[Back to README](../README.md#documentation).

These are historical measurements. The repository no longer includes the
scripts that produced them.

## Benchmark results

We recorded these measurements on 2026-09-11. The system used Linux x86-64,
Elixir 1.20.4, OTP 29, and release builds with LanceDB 0.37.1. MinIO ran in
a temporary local Docker container. These measurements describe local
development runs. They do not estimate production capacity. We have not
repeated these measurements for the latest cursor scheduler changes.

### Row input transport

Each mode and workload ran in a fresh BEAM VM. Each run used 2,000 rows, one
warmup, and five measured conversions. The script then checked exact
results. Vector rows had 384 float32 components. Binary rows had 1,024
arbitrary bytes. Nested rows had a string and 20 integer list elements.

Both row transports used the same validation and Arrow decoding.

Times below are totals for five conversions. RSS means resident set size.
Peak RSS includes BEAM, native memory, input, warmup, and result checks.

| Workload | JSON input | Direct terms | Term speedup | JSON/term peak RSS |
| --- | ---: | ---: | ---: | ---: |
| Vectors | 792.25 ms | 344.39 ms | 2.30× | 730,760 / 573,752 KiB |
| Binary | 152.42 ms | 121.49 ms | 1.25× | 187,712 / 191,404 KiB |
| Nested | 74.01 ms | 53.38 ms | 1.39× | 179,972 / 173,368 KiB |

These results support direct term input for row ingestion. The binary case
did not show a memory improvement. The internal JSON adapter stays available
for comparisons. Small control and schema descriptions still use JSON.

Arrow to IPC to Arrow took 8.24 ms for vectors, 8.90 ms for binary, and 0.64
ms for nested batches. These runs started with existing Arrow data. They do
not measure row ingestion. Use IPC for interchange and native batches for
direct query-to-ingestion transfer.

### Complete copies and concurrent reads

Copy runs used 1,000 rows with IDs, strings, and 32-component vectors. Each
copy used four demand batches and one destination commit. Both methods
queried the source and checked the destination row count. Row mode decoded
and encoded records. Batch mode kept Arrow data.

| Storage | Row stream copy | Arrow batch copy |
| --- | ---: | ---: |
| Local | 14.88 ms | 1.21 ms |
| MinIO | 21.90 ms | 10.37 ms |

Each concurrent read run used 64 scans of 32 rows after a warmup. Higher
concurrency improved local throughput. It increased latency and reduced
MinIO throughput for this workload.

| Storage | Concurrency | Total | p50 | p95 |
| --- | ---: | ---: | ---: | ---: |
| Local | 1 | 27.01 ms | 0.39 ms | 0.48 ms |
| Local | 4 | 10.91 ms | 0.65 ms | 0.76 ms |
| Local | 16 | 5.78 ms | 1.14 ms | 1.65 ms |
| MinIO | 1 | 62.77 ms | 0.87 ms | 1.12 ms |
| MinIO | 4 | 82.21 ms | 5.70 ms | 6.63 ms |
| MinIO | 16 | 96.06 ms | 20.82 ms | 27.53 ms |

These measurements compare copy methods and read concurrency before the
latest cursor scheduler changes. They do not measure scheduler costs or
identify the best admission limits. Select limits for your workload, storage
latency, and available memory. Tests check busy errors, slot release, and
transfer from a reader to a writer.

### Raw measurements

The following JSON contains the recorded measurements.

<details>
<summary>Raw benchmark measurements</summary>

```json
{
  "date": "2026-09-11",
  "transport": [
    {
      "mode": "json",
      "rows": 2000,
      "milliseconds": 792.25,
      "workload": "vectors",
      "repeats": 5,
      "rows_per_second": 12622.278321236983,
      "process_peak_rss_kib": 730760
    },
    {
      "mode": "terms",
      "rows": 2000,
      "milliseconds": 344.386,
      "workload": "vectors",
      "repeats": 5,
      "rows_per_second": 29037.185019135504,
      "process_peak_rss_kib": 573752
    },
    {
      "mode": "ipc",
      "rows": 2000,
      "milliseconds": 8.237,
      "workload": "vectors",
      "repeats": 5,
      "rows_per_second": 1214034.2357654485,
      "process_peak_rss_kib": 425812
    },
    {
      "mode": "json",
      "rows": 2000,
      "milliseconds": 152.423,
      "workload": "binary",
      "repeats": 5,
      "rows_per_second": 65606.89659697027,
      "process_peak_rss_kib": 187712
    },
    {
      "mode": "terms",
      "rows": 2000,
      "milliseconds": 121.486,
      "workload": "binary",
      "repeats": 5,
      "rows_per_second": 82314.011491036,
      "process_peak_rss_kib": 191404
    },
    {
      "mode": "ipc",
      "rows": 2000,
      "milliseconds": 8.901,
      "workload": "binary",
      "repeats": 5,
      "rows_per_second": 1123469.2731153802,
      "process_peak_rss_kib": 185708
    },
    {
      "mode": "json",
      "rows": 2000,
      "milliseconds": 74.014,
      "workload": "nested",
      "repeats": 5,
      "rows_per_second": 135109.57386440402,
      "process_peak_rss_kib": 179972
    },
    {
      "mode": "terms",
      "rows": 2000,
      "milliseconds": 53.381,
      "workload": "nested",
      "repeats": 5,
      "rows_per_second": 187332.57151420918,
      "process_peak_rss_kib": 173368
    },
    {
      "mode": "ipc",
      "rows": 2000,
      "milliseconds": 0.643,
      "workload": "nested",
      "repeats": 5,
      "rows_per_second": 15552099.533437014,
      "process_peak_rss_kib": 167596
    }
  ],
  "local": [
    {
      "concurrency": 1,
      "workload": "scan",
      "storage": "local",
      "queries": 64,
      "total_ms": 27.009,
      "p50_ms": 0.394,
      "p95_ms": 0.478
    },
    {
      "concurrency": 4,
      "workload": "scan",
      "storage": "local",
      "queries": 64,
      "total_ms": 10.91,
      "p50_ms": 0.647,
      "p95_ms": 0.761
    },
    {
      "concurrency": 16,
      "workload": "scan",
      "storage": "local",
      "queries": 64,
      "total_ms": 5.783,
      "p50_ms": 1.143,
      "p95_ms": 1.65
    },
    {
      "mode": "rows",
      "rows": 1000,
      "workload": "copy",
      "storage": "local",
      "total_ms": 14.875
    },
    {
      "mode": "batches",
      "rows": 1000,
      "workload": "copy",
      "storage": "local",
      "total_ms": 1.209
    }
  ],
  "minio": [
    {
      "concurrency": 1,
      "workload": "scan",
      "storage": "minio",
      "queries": 64,
      "total_ms": 62.771,
      "p50_ms": 0.874,
      "p95_ms": 1.124
    },
    {
      "concurrency": 4,
      "workload": "scan",
      "storage": "minio",
      "queries": 64,
      "total_ms": 82.208,
      "p50_ms": 5.696,
      "p95_ms": 6.626
    },
    {
      "concurrency": 16,
      "workload": "scan",
      "storage": "minio",
      "queries": 64,
      "total_ms": 96.06,
      "p50_ms": 20.824,
      "p95_ms": 27.526
    },
    {
      "mode": "rows",
      "rows": 1000,
      "workload": "copy",
      "storage": "minio",
      "total_ms": 21.895
    },
    {
      "mode": "batches",
      "rows": 1000,
      "workload": "copy",
      "storage": "minio",
      "total_ms": 10.367
    }
  ]
}
```

</details>
