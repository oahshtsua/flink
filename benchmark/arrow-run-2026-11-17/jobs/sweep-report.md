## Whole jobs (local MiniCluster)

Median of 5 runs. ns per input record (the mixed job's flatMap doubles the records after it). B = records per batch.

### simple: map → filter (200,000,000 records)

| variant | B | ms | ns/record | vs stock |
|---|---|---|---|---|
| original job, stock Flink |  | 4101 | 20.5 | 1.00× |
| original job, fusion build |  | 3536 | 17.7 | 1.16× |
| Arrow batches, functions rewritten | 64 | 979 | 4.9 | 4.19× |
| Arrow batches, functions rewritten | 256 | 775 | 3.9 | 5.29× |
| Arrow batches, functions rewritten | 1024 | 707 | 3.5 | 5.80× |
| Arrow batches, functions rewritten | 4096 | 683 | 3.4 | 6.00× |
| Arrow batches, functions rewritten | 16384 | 685 | 3.4 | 5.99× |
| Arrow batches, functions wrapped per row | 64 | 1901 | 9.5 | 2.16× |
| Arrow batches, functions wrapped per row | 256 | 1735 | 8.7 | 2.36× |
| Arrow batches, functions wrapped per row | 1024 | 1740 | 8.7 | 2.36× |
| Arrow batches, functions wrapped per row | 4096 | 1737 | 8.7 | 2.36× |
| Arrow batches, functions wrapped per row | 16384 | 1775 | 8.9 | 2.31× |

### mixed: flatMap → map → filter → map (200,000,000 records)

| variant | B | ms | ns/record | vs stock |
|---|---|---|---|---|
| original job, stock Flink |  | 10948 | 54.7 | 1.00× |
| original job, fusion build |  | 7951 | 39.8 | 1.38× |
| Arrow batches, functions rewritten | 64 | 1336 | 6.7 | 8.19× |
| Arrow batches, functions rewritten | 256 | 1104 | 5.5 | 9.92× |
| Arrow batches, functions rewritten | 1024 | 1066 | 5.3 | 10.27× |
| Arrow batches, functions rewritten | 4096 | 1046 | 5.2 | 10.47× |
| Arrow batches, functions rewritten | 16384 | 1052 | 5.3 | 10.41× |
| Arrow batches, functions wrapped per row | 64 | 4774 | 23.9 | 2.29× |
| Arrow batches, functions wrapped per row | 256 | 4420 | 22.1 | 2.48× |
| Arrow batches, functions wrapped per row | 1024 | 4403 | 22.0 | 2.49× |
| Arrow batches, functions wrapped per row | 4096 | 4275 | 21.4 | 2.56× |
| Arrow batches, functions wrapped per row | 16384 | 4308 | 21.5 | 2.54× |

### sensor: map → filter → map → filter (50,000,000 records)

ns/record (speed-up vs stock):

| variant | B | k=1 | k=64 |
|---|---|---|---|
| original job, stock Flink |  | 127.3 (1.00×) | 262.6 (1.00×) |
| original job, fusion build |  | 126.6 (1.01×) | 247.9 (1.06×) |
| Arrow batches, functions rewritten | 64 | 66.9 (1.90×) | 215.5 (1.22×) |
| Arrow batches, functions rewritten | 256 | 87.8 (1.45×) | 235.4 (1.12×) |
| Arrow batches, functions rewritten | 1024 | 87.5 (1.46×) | 235.2 (1.12×) |

Run-to-run spread (max-min over median): typical 3.8%, worst 30.5% (mixed arrow-wrapped, B=64).
