package org.wso2.siddhi.debs2017.extension;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.selector.attribute.aggregator.AttributeAggregator;
import org.wso2.siddhi.debs2017.query.SingleNodeServer;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.ArrayList;

/*
* Copyright (c) 2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

/**
 * TimestampAggregator
 */
public class TimestampAggregator extends AttributeAggregator {
    ArrayList<String> arr = new ArrayList<>();

    @Override
    protected void init(ExpressionExecutor[] expressionExecutors, ExecutionPlanContext
            executionPlanContext) {

    }

    @Override
    public Attribute.Type getReturnType() {
        return null;
    }

    @Override
    public Object processAdd(Object o) {
        arr.add(o.toString());
        if (arr.size() == SingleNodeServer.windowSize) {
            return arr.get(SingleNodeServer.windowSize - SingleNodeServer.transitionsCount - 1);
        }
        return arr.get(arr.size() - 1);
    }

    @Override
    public Object processAdd(Object[] objects) {
        return null;
    }

    @Override
    public Object processRemove(Object o) {
        arr.remove(0);
        return null;
    }

    @Override
    public Object processRemove(Object[] objects) {
        return null;
    }

    @Override
    public Object reset() {
        return null;
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public Object[] currentState() {
        return new Object[0];
    }

    @Override
    public void restoreState(Object[] objects) {

    }
}
