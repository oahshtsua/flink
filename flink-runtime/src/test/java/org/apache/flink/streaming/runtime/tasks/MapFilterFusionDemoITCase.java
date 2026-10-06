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

import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;

import org.junit.jupiter.api.Test;

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
 * <p>Look for the {@code [CustomOptimizer] Fusing chained Map (...) -> Filter (...)} log line when
 * running this test to see the hook fire.
 */
class MapFilterFusionDemoITCase {

    private static final List<Integer> RESULTS = Collections.synchronizedList(new ArrayList<>());

    @Test
    void mapFilterChainIsFusedByCustomOptimizer() throws Exception {
        RESULTS.clear();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // The prototype fusion only engages with object reuse enabled; see isMapFilterFusable.
        env.getConfig().enableObjectReuse();

        env.fromData(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
                .map(x -> x * 2)
                .filter(x -> x % 3 == 0)
                .addSink(new CollectingSink());

        StreamGraph streamGraph = env.getStreamGraph();
        JobGraph jobGraph = streamGraph.getJobGraph(getClass().getClassLoader());

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

        // 1..10 -> map(x*2) -> 2,4,6,8,10,12,14,16,18,20 -> filter(%3==0) -> 6,12,18
        assertThat(RESULTS).containsExactlyInAnyOrder(6, 12, 18);
    }

    private static class CollectingSink implements SinkFunction<Integer> {
        private static final long serialVersionUID = 1L;

        @Override
        public void invoke(Integer value, Context context) {
            RESULTS.add(value);
        }
    }
}
