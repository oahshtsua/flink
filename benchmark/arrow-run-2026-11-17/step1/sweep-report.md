## Arrow zero-copy vs copying native operator (local MiniCluster, whole job)

ns per record, median of 3 runs. Includes the data generation.

| k | tuned Java | native B=64 | native B=256 | native B=1024 | native B=4096 | arrow B=64 | arrow B=256 | arrow B=1024 | arrow B=4096 |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 124 | 126 | 136 | 128 | 130 | 157 | 108 | 90 | 87 |
| 4 | 129 | 132 | 138 | 138 | 132 | 184 | 113 | 95 | 92 |
| 16 | 156 | 151 | 160 | 160 | 154 | 185 | 134 | 115 | 110 |
| 64 | 229 | 214 | 211 | 214 | 212 | 238 | 190 | 169 | 169 |

**Arrow speed-up over the copying native operator at the same batch size** (>1 = Arrow faster):

| k | B=64 | B=256 | B=1024 | B=4096 |
|---|---|---|---|---|
| 1 | 0.80× | 1.26× | 1.41× | 1.50× |
| 4 | 0.72× | 1.22× | 1.45× | 1.44× |
| 16 | 0.82× | 1.20× | 1.39× | 1.40× |
| 64 | 0.90× | 1.11× | 1.26× | 1.26× |

**Speed-up over tuned Java** (>1 = faster than tuned Java):

| k | native B=64 | native B=256 | native B=1024 | native B=4096 | arrow B=64 | arrow B=256 | arrow B=1024 | arrow B=4096 |
|---|---|---|---|---|---|---|---|---|
| 1 | 0.99× | 0.91× | 0.97× | 0.95× | 0.79× | 1.15× | 1.37× | 1.43× |
| 4 | 0.98× | 0.94× | 0.94× | 0.98× | 0.70× | 1.14× | 1.36× | 1.41× |
| 16 | 1.03× | 0.98× | 0.98× | 1.01× | 0.84× | 1.17× | 1.36× | 1.42× |
| 64 | 1.07× | 1.09× | 1.07× | 1.08× | 0.97× | 1.20× | 1.36× | 1.36× |

Run-to-run spread (max-min over median): typical 4.5%, worst 23.3% (arrow, k=4, B=256).
