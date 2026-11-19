## Inside Flink (local MiniCluster, whole job)

ns per record, median of 3 runs. Includes the data generation. B = readings per batch.

| variant | B | k=1 | k=4 | k=16 | k=64 |
|---|---|---|---|---|---|
| tuned Java (one record at a time) |  | 125 | 127 | 159 | 231 |
| copying native (idea 4) | 64 | 127 | 132 | 154 | 210 |
| copying native (idea 4) | 256 | 132 | 138 | 150 | 212 |
| copying native (idea 4) | 1024 | 126 | 138 | 156 | 214 |
| copying native (idea 4) | 4096 | 132 | 134 | 157 | 213 |
| Arrow + C | 64 | 153 | 160 | 202 | 244 |
| Arrow + C | 256 | 108 | 113 | 132 | 188 |
| Arrow + C | 1024 | 89 | 95 | 113 | 169 |
| Arrow + C | 4096 | 84 | 90 | 109 | 164 |
| Arrow + Java, one function | 64 | 66 | 90 | 116 | 179 |
| Arrow + Java, one function | 256 | 87 | 110 | 138 | 202 |
| Arrow + Java, one function | 1024 | 87 | 110 | 137 | 200 |
| Arrow + Java, one function | 4096 | 88 | 111 | 137 | 200 |
| Arrow + Java, four steps | 64 | 68 | 88 | 120 | 209 |
| Arrow + Java, four steps | 256 | 64 | 84 | 119 | 231 |
| Arrow + Java, four steps | 1024 | 87 | 106 | 140 | 229 |
| Arrow + Java, four steps | 4096 | 86 | 106 | 141 | 232 |

**Speed-up over tuned Java** (>1 = faster than tuned Java):

| variant | B | k=1 | k=4 | k=16 | k=64 |
|---|---|---|---|---|---|
| copying native (idea 4) | 64 | 0.98× | 0.97× | 1.03× | 1.10× |
| copying native (idea 4) | 256 | 0.95× | 0.92× | 1.06× | 1.09× |
| copying native (idea 4) | 1024 | 0.99× | 0.92× | 1.02× | 1.08× |
| copying native (idea 4) | 4096 | 0.94× | 0.95× | 1.01× | 1.08× |
| Arrow + C | 64 | 0.81× | 0.80× | 0.79× | 0.95× |
| Arrow + C | 256 | 1.15× | 1.13× | 1.20× | 1.23× |
| Arrow + C | 1024 | 1.40× | 1.34× | 1.40× | 1.37× |
| Arrow + C | 4096 | 1.48× | 1.41× | 1.46× | 1.41× |
| Arrow + Java, one function | 64 | 1.88× | 1.42× | 1.37× | 1.29× |
| Arrow + Java, one function | 256 | 1.43× | 1.15× | 1.15× | 1.15× |
| Arrow + Java, one function | 1024 | 1.43× | 1.16× | 1.16× | 1.15× |
| Arrow + Java, one function | 4096 | 1.42× | 1.15× | 1.16× | 1.16× |
| Arrow + Java, four steps | 64 | 1.84× | 1.45× | 1.32× | 1.11× |
| Arrow + Java, four steps | 256 | 1.93× | 1.52× | 1.34× | 1.00× |
| Arrow + Java, four steps | 1024 | 1.43× | 1.20× | 1.14× | 1.01× |
| Arrow + Java, four steps | 4096 | 1.45× | 1.21× | 1.12× | 1.00× |

**Against Arrow + C at the same B** (>1 = faster than Arrow + C):

| variant | B | k=1 | k=4 | k=16 | k=64 |
|---|---|---|---|---|---|
| copying native (idea 4) | 64 | 1.21× | 1.21× | 1.31× | 1.17× |
| copying native (idea 4) | 256 | 0.82× | 0.81× | 0.88× | 0.89× |
| copying native (idea 4) | 1024 | 0.71× | 0.69× | 0.73× | 0.79× |
| copying native (idea 4) | 4096 | 0.64× | 0.68× | 0.69× | 0.77× |
| Arrow + Java, one function | 64 | 2.31× | 1.79× | 1.73× | 1.37× |
| Arrow + Java, one function | 256 | 1.25× | 1.02× | 0.96× | 0.93× |
| Arrow + Java, one function | 1024 | 1.02× | 0.86× | 0.83× | 0.84× |
| Arrow + Java, one function | 4096 | 0.96× | 0.82× | 0.79× | 0.82× |
| Arrow + Java, four steps | 64 | 2.27× | 1.82× | 1.68× | 1.17× |
| Arrow + Java, four steps | 256 | 1.68× | 1.35× | 1.11× | 0.81× |
| Arrow + Java, four steps | 1024 | 1.03× | 0.89× | 0.81× | 0.74× |
| Arrow + Java, four steps | 4096 | 0.98× | 0.85× | 0.77× | 0.71× |

Run-to-run spread (max-min over median): typical 3.3%, worst 35.5% (arrow-steps, k=1, B=256).
