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
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Prototype fusion target for a chained {@link StreamMap} immediately followed by a {@link
 * StreamFilter}: runs both user functions inside one {@code processElement} call instead of routing
 * the record through a {@link org.apache.flink.streaming.runtime.tasks.ChainingOutput} hop between
 * two separate operators. Spliced in by {@link
 * org.apache.flink.streaming.runtime.tasks.OperatorChain} once the chain has been built.
 */
@Internal
public class MapFilterFusedOperator<IN, OUT> extends AbstractStreamOperator<OUT>
        implements OneInputStreamOperator<IN, OUT> {

    private static final long serialVersionUID = 1L;

    private final MapFunction<IN, OUT> mapFunction;
    private final FilterFunction<OUT> filterFunction;

    public MapFilterFusedOperator(
            MapFunction<IN, OUT> mapFunction, FilterFunction<OUT> filterFunction) {
        this.mapFunction = mapFunction;
        this.filterFunction = filterFunction;
    }

    @Override
    public void open() throws Exception {
        super.open();
        FunctionUtils.setFunctionRuntimeContext(mapFunction, getRuntimeContext());
        FunctionUtils.setFunctionRuntimeContext(filterFunction, getRuntimeContext());
        FunctionUtils.openFunction(mapFunction, DefaultOpenContext.INSTANCE);
        FunctionUtils.openFunction(filterFunction, DefaultOpenContext.INSTANCE);
    }

    @Override
    public void close() throws Exception {
        try {
            FunctionUtils.closeFunction(mapFunction);
        } finally {
            FunctionUtils.closeFunction(filterFunction);
        }
        super.close();
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        OUT mapped = mapFunction.map(element.getValue());
        if (filterFunction.filter(mapped)) {
            output.collect(element.replace(mapped));
        }
    }
}
