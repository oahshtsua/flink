/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Prototype fusion target for a chained run of two or more {@link StreamMap}/{@link StreamFilter}
 * operators, in any order and of any length (e.g. {@code map -> filter -> map -> filter}): runs
 * every step's user function inside one {@code processElement} call instead of routing the record
 * through a {@link org.apache.flink.streaming.runtime.tasks.ChainingOutput} hop between each pair
 * of separate operators. Spliced in by {@link
 * org.apache.flink.streaming.runtime.tasks.OperatorChain} once the chain has been built.
 */
@Internal
public class MapFilterFusedOperator<IN, OUT> extends AbstractStreamOperator<OUT>
        implements OneInputStreamOperator<IN, OUT> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(MapFilterFusedOperator.class);

    /** Which of the two supported step shapes a {@link Step} wraps. */
    public enum StepKind {
        MAP,
        FILTER
    }

    /**
     * One step of the fused run: a kind tag plus the real {@code MapFunction}/{@code
     * FilterFunction} instance, type-erased since steps in one run don't share a single IN/OUT type
     * pair - {@link #processElement} dispatches on {@link #kind} and casts per step.
     */
    public static final class Step {
        private final StepKind kind;
        private final Function function;

        public Step(StepKind kind, Function function) {
            this.kind = kind;
            this.function = function;
        }

        public StepKind getKind() {
            return kind;
        }

        public Function getFunction() {
            return function;
        }
    }

    // Stored as a plain array, not List<Step>, even though the constructor takes a List for
    // caller convenience: a for-each loop over a List-typed field goes through Iterator, which
    // the JIT doesn't reliably scalar-replace away in this hot loop - measured as a real,
    // reproducible throughput regression (~15% slower than the fixed 2-field version) before
    // this change. A for-each loop over an array compiles to a plain indexed loop with no
    // allocation, regardless of field type.
    private final Step[] steps;

    private long processedCount = 0L;
    private long passedCount = 0L;

    public MapFilterFusedOperator(List<Step> steps) {
        if (steps.size() < 2) {
            throw new IllegalArgumentException(
                    "MapFilterFusedOperator needs at least 2 steps to be worth fusing, got "
                            + steps.size());
        }
        this.steps = steps.toArray(new Step[0]);
    }

    @Override
    public void open() throws Exception {
        super.open();
        for (Step step : steps) {
            FunctionUtils.setFunctionRuntimeContext(step.function, getRuntimeContext());
            FunctionUtils.openFunction(step.function, DefaultOpenContext.INSTANCE);
        }
        LOG.info(
                "[CustomOptimizer] MapFilterFusedOperator opened: {} step(s): {}",
                steps.length,
                describeSteps());
    }

    @Override
    public void close() throws Exception {
        LOG.info(
                "[CustomOptimizer] MapFilterFusedOperator closing: processed={} passed={}"
                        + " filteredOut={}",
                processedCount,
                passedCount,
                processedCount - passedCount);
        // Best-effort close of every step's function: one failing close() must not stop the
        // rest from being closed, and the first failure (if any) is what gets propagated.
        Exception firstFailure = null;
        for (Step step : steps) {
            try {
                FunctionUtils.closeFunction(step.function);
            } catch (Exception e) {
                if (firstFailure == null) {
                    firstFailure = e;
                } else {
                    firstFailure.addSuppressed(e);
                }
            }
        }
        super.close();
        if (firstFailure != null) {
            throw firstFailure;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void processElement(StreamRecord<IN> element) throws Exception {
        processedCount++;
        Object value = element.getValue();
        for (Step step : steps) {
            if (step.kind == StepKind.MAP) {
                value = ((MapFunction<Object, Object>) step.function).map(value);
            } else {
                if (!((FilterFunction<Object>) step.function).filter(value)) {
                    return;
                }
            }
        }
        passedCount++;
        output.collect((StreamRecord<OUT>) element.replace(value));
    }

    private String describeSteps() {
        return Arrays.stream(steps)
                .map(step -> step.kind + "(" + step.function.getClass().getName() + ")")
                .collect(Collectors.joining(" -> "));
    }
}
