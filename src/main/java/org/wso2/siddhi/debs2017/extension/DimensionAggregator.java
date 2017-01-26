package org.wso2.siddhi.debs2017.extension;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.selector.attribute.aggregator.AttributeAggregator;
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
public class DimensionAggregator extends AttributeAggregator{
    private ArrayList<Double> arr = new ArrayList<Double>();
    protected void init(ExpressionExecutor[] expressionExecutors, ExecutionPlanContext executionPlanContext) {

    }

    public Attribute.Type getReturnType() {
        return null;
    }

    public Object processAdd(Object data) {
        arr.add((Double)data );
        // Do the clustering on the elements present in the array list and return the center it belongs to
        return "center";
    }

    public Object processAdd(Object[] objects) {

        return null;
    }

    public Object processRemove(Object o) {
        //removes expired events
        arr.remove(0);
        return null;
    }

    public Object processRemove(Object[] objects) {
        return null;
    }

    public Object reset() {
        return null;
    }

    public void start() {

    }

    public void stop() {

    }

    public Object[] currentState() {
        return new Object[0];
    }

    public void restoreState(Object[] objects) {

    }
}
