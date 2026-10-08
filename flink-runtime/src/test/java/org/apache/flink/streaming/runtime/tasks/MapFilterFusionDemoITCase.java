/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.testutils.logging.LoggerAuditingExtension;
import org.apache.flink.util.Collector;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end demo of a prototype custom optimizer: a real {@link StreamExecutionEnvironment} job
 * containing a run of chained {@code map()}/{@code filter()} operators is turned into a real {@link
 * StreamGraph} and {@link JobGraph} (untouched, stock Flink chaining decisions), then run on a real
 * {@link MiniCluster}. The fusion itself happens inside {@link OperatorChain} once the job's chain
 * is built, right before the chain starts processing records (see {@code findFusableRun} / {@code
 * createFusedOperatorChainOutput}): any maximal chained run of two or more {@code Map}/{@code
 * Filter} operators, in any order and of any length, is replaced with a single fused operator.
 *
 * <p>Each test asserts two independent things, neither of which alone would be convincing: the
 * job's actual output is exactly what the untouched transform would produce (correctness), and the
 * {@code [CustomOptimizer] Fusing chained ...} log line(s) fired exactly the expected number of
 * times, each covering exactly the expected run length (proof the fusion engaged on exactly the
 * right span of operators - not fewer, not more, not merged across an operator that should have
 * stopped it), via {@link LoggerAuditingExtension} rather than requiring a human to eyeball the log
 * output.
 */
class MapFilterFusionDemoITCase {

    @RegisterExtension
    private final LoggerAuditingExtension operatorChainLogs =
            new LoggerAuditingExtension(OperatorChain.class, org.slf4j.event.Level.INFO);

    private static final List<Object> RESULTS = Collections.synchronizedList(new ArrayList<>());

    @BeforeEach
    void clearResults() {
        RESULTS.clear();
    }

    private long fusionEventCount() {
        return operatorChainLogs.getMessages().stream()
                .filter(m -> m.contains("[CustomOptimizer] Fusing chained"))
                .count();
    }

    /** How many fusion events fused a run of exactly {@code size} operators together. */
    private long fusionEventCountForRunSize(int size) {
        String marker = "Fusing chained run of " + size + " operators";
        return operatorChainLogs.getMessages().stream().filter(m -> m.contains(marker)).count();
    }

    @Test
    void mapFilterChainIsFusedByCustomOptimizer() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // The prototype fusion only engages with object reuse enabled; see isMapFilterFusable.
        env.getConfig().enableObjectReuse();

        env.fromData(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
                .map(x -> x * 2)
                .filter(x -> x % 3 == 0)
                .addSink(new CollectingSink<Integer>());

        run(env);

        // 1..10 -> map(x*2) -> 2,4,6,8,10,12,14,16,18,20 -> filter(%3==0) -> 6,12,18
        assertThat(RESULTS).containsExactlyInAnyOrder(6, 12, 18);
        assertThat(fusionEventCount()).isEqualTo(1);
    }

    @Test
    void complexMapAndFilterOverNonTrivialTypeIsFused() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().enableObjectReuse();

        env.fromData("alpha:3", "beta:12", "gamma:7", "delta:20", "epsilon:5", "zeta:18")
                .map(new ParseNameAndSquare())
                .filter(t -> t.f1 > 50 && t.f0.length() % 2 == 0)
                .addSink(new CollectingSink<Tuple2<String, Integer>>());

        run(env);

        // "name:n" -> (name, n*n), kept when n*n > 50 AND name has an even number of letters:
        //   alpha:3   -> (alpha,    9) - 9 not > 50                           -> dropped
        //   beta:12   -> (beta,   144) - 144 > 50, "beta" has 4 letters       -> kept
        //   gamma:7   -> (gamma,   49) - 49 not > 50                         -> dropped
        //   delta:20  -> (delta,  400) - 400 > 50, but "delta" has 5 letters -> dropped
        //   epsilon:5 -> (epsilon,25) - 25 not > 50                         -> dropped
        //   zeta:18   -> (zeta,   324) - 324 > 50, "zeta" has 4 letters      -> kept
        assertThat(RESULTS)
                .containsExactlyInAnyOrder(Tuple2.of("beta", 144), Tuple2.of("zeta", 324));
        assertThat(fusionEventCount()).isEqualTo(1);
    }

    @Test
    void nonMapFilterOperatorIsLeftOutOfTheFusedRun() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().enableObjectReuse();

        env.fromData(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
                .flatMap(new DuplicateShifted())
                .map(x -> x * 2)
                .filter(x -> x % 3 == 0)
                .map(x -> "v" + x)
                .addSink(new CollectingSink<String>());

        run(env);

        // flatMap expands 1..10 into {1..10, 101..110} - untouched by the optimizer, since it's
        // neither a Map nor a Filter. Everything after it - map(x*2), filter(%3==0), map("v"+x) -
        // IS a chained run of nothing but Map/Filter operators, so all three fuse into one
        // operator (not just the map->filter pair - the trailing map continues the same run):
        //   from 1..10:    2,4,...,20   -> keep 6,12,18     -> "v6","v12","v18"
        //   from 101..110: 202,...,220  -> keep 204,210,216 -> "v204","v210","v216"
        assertThat(RESULTS).containsExactlyInAnyOrder("v6", "v12", "v18", "v204", "v210", "v216");
        // One fusion event, covering all 3 trailing operators - the flatMap is the only thing
        // that stays a separate operator. (Not 2 events, which would mean the run got split
        // instead of recognizing map->filter->map as one continuous chain.)
        assertThat(fusionEventCount()).isEqualTo(1);
        assertThat(fusionEventCountForRunSize(3)).isEqualTo(1);
    }

    @Test
    void longRunOfMapAndFilterOperatorsFusesIntoOneOperator() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().enableObjectReuse();

        env.fromData(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
                .map(x -> x + 1)
                .filter(x -> x % 2 == 0)
                .map(x -> x * 10)
                .filter(x -> x < 80)
                .map(x -> x - 5)
                .addSink(new CollectingSink<Integer>());

        run(env);

        // A 5-operator run (map, filter, map, filter, map), nothing interrupting it - the
        // fusion pattern isn't limited to exactly 2 steps, it matches a chained run of any
        // length as long as every step is a Map or a Filter:
        //   1..10 -> (+1) -> 2..11 -> (keep even) -> 2,4,6,8,10
        //         -> (*10) -> 20,40,60,80,100 -> (keep <80) -> 20,40,60
        //         -> (-5) -> 15,35,55
        assertThat(RESULTS).containsExactlyInAnyOrder(15, 35, 55);
        // Exactly one fusion event, and it covers all 5 operators - not e.g. two separate
        // 2-and-3 or 2-and-2-and-1 fusions, proving the whole run collapses into one operator.
        assertThat(fusionEventCount()).isEqualTo(1);
        assertThat(fusionEventCountForRunSize(5)).isEqualTo(1);
    }

    @Test
    void nonFusableOperatorInTheMiddleSplitsOneRunIntoTwo() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().enableObjectReuse();

        env.fromData(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
                .map(x -> x * 3)
                .filter(x -> x % 2 == 0)
                .flatMap(new DuplicateShifted(1000))
                .map(x -> x + 1)
                .filter(x -> x > 20)
                .addSink(new CollectingSink<Integer>());

        run(env);

        // Two separate 2-operator runs (map->filter, then map->filter again), with a flatMap
        // in between that cannot be part of either - the detector must not merge across it:
        //   1..10 -> (*3) -> 3..30 -> (keep even) -> 6,12,18,24,30
        //         -> flatMap(x, x+1000) -> 6,12,18,24,30, 1006,1012,1018,1024,1030
        //         -> (+1) -> 7,13,19,25,31, 1007,1013,1019,1025,1031
        //         -> (keep >20) -> 25,31, 1007,1013,1019,1025,1031
        assertThat(RESULTS).containsExactlyInAnyOrder(25, 31, 1007, 1013, 1019, 1025, 1031);
        // Two fusion events, each covering exactly 2 operators - not one event spanning both
        // sides of the flatMap, and not the flatMap itself ending up folded into either run.
        assertThat(fusionEventCount()).isEqualTo(2);
        assertThat(fusionEventCountForRunSize(2)).isEqualTo(2);
    }

    private static void run(StreamExecutionEnvironment env) throws Exception {
        StreamGraph streamGraph = env.getStreamGraph();
        JobGraph jobGraph =
                streamGraph.getJobGraph(MapFilterFusionDemoITCase.class.getClassLoader());

        MiniClusterConfiguration cfg =
                new MiniClusterConfiguration.Builder()
                        .withRandomPorts()
                        .setNumTaskManagers(1)
                        .setNumSlotsPerTaskManager(1)
                        .build();

        try (MiniCluster miniCluster = new MiniCluster(cfg)) {
            miniCluster.start();
            miniCluster.executeJobBlocking(jobGraph);
        }
    }

    private static class ParseNameAndSquare
            implements MapFunction<String, Tuple2<String, Integer>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Tuple2<String, Integer> map(String value) {
            String[] parts = value.split(":");
            int n = Integer.parseInt(parts[1]);
            return Tuple2.of(parts[0], n * n);
        }
    }

    private static class DuplicateShifted implements FlatMapFunction<Integer, Integer> {
        private static final long serialVersionUID = 1L;

        private final int shift;

        DuplicateShifted() {
            this(100);
        }

        DuplicateShifted(int shift) {
            this.shift = shift;
        }

        @Override
        public void flatMap(Integer value, Collector<Integer> out) {
            out.collect(value);
            out.collect(value + shift);
        }
    }

    private static class CollectingSink<T> implements SinkFunction<T> {
        private static final long serialVersionUID = 1L;

        @Override
        public void invoke(T value, Context context) {
            RESULTS.add(value);
        }
    }
}
