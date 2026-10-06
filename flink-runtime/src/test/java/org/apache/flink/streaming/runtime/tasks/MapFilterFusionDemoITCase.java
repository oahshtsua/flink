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
 * containing a {@code map().filter()} chain is turned into a real {@link StreamGraph} and {@link
 * JobGraph} (untouched, stock Flink chaining decisions), then run on a real {@link MiniCluster}.
 * The fusion itself happens inside {@link OperatorChain} once the job's chain is built, right
 * before the chain starts processing records (see {@code isMapFilterFusable} / {@code
 * createFusedMapFilterOutput}).
 *
 * <p>Each test asserts two independent things, neither of which alone would be convincing: the
 * job's actual output is exactly what the untouched transform would produce (correctness), and the
 * {@code [CustomOptimizer] Fusing chained ...} log line fired exactly the expected number of times
 * (proof the fusion engaged - or, where other operators are mixed in, engaged for only the one
 * adjacent Map-Filter pair and left everything else alone), via {@link LoggerAuditingExtension}
 * rather than requiring a human to eyeball the log output.
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
    void onlyTheAdjacentMapFilterPairIsFusedAmongOtherOperators() throws Exception {
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
        // neither a Map nor a Filter. Only the map(x*2) -> filter(%3==0) pair is an immediately
        // chained Map followed by a Filter, so it's the one pair the fusion pattern matches:
        //   from 1..10:    2,4,...,20   -> keep 6,12,18
        //   from 101..110: 202,...,220  -> keep 204,210,216
        // The trailing map(x -> "v"+x) comes right after the Filter, not before it, so it does
        // NOT match the (Map -> Filter) pattern and runs as an ordinary, separate operator.
        assertThat(RESULTS).containsExactlyInAnyOrder("v6", "v12", "v18", "v204", "v210", "v216");
        // Exactly one Map->Filter pair in this five-operator chain matches the fusion pattern -
        // the flatMap before it and the map after the filter are left as separate operators.
        assertThat(fusionEventCount()).isEqualTo(1);
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

        @Override
        public void flatMap(Integer value, Collector<Integer> out) {
            out.collect(value);
            out.collect(value + 100);
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
