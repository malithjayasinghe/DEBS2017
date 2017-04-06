/*
 *
 *  Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * /
 *
 */

package org.wso2.siddhi.debs2017.extension;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.debs2017.markovchain.Markovnew;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.ArrayList;


public class MarkovFunction extends FunctionExecutor {
    Markovnew markovnew = new Markovnew();
    private ArrayList<Integer> centers;
    private  double probability = -1;
    @Override
    protected void init(ExpressionExecutor[] expressionExecutors, ExecutionPlanContext executionPlanContext) {

    }

    @Override
    protected Object execute(Object[] objects) {
        centers = (ArrayList<Integer>)objects[0];

        if(centers.size()>=((Integer)objects[1])){
            markovnew.execute(centers);
            //get the total probability
            probability =  markovnew.updateProbability(centers);


        } else {
            return 2.0;
        }
        return probability;


    }

    @Override
    protected Object execute(Object o) {
        centers = (ArrayList<Integer>)o;

        if(centers.size() == 2)
           probability = 1;
       else if(centers.size() >2){
           //update the transitional probabilities
           markovnew.execute(centers);

           //get the total probability
           probability =  markovnew.updateProbability(centers);

       }

        return probability;
    }

    @Override
    public Attribute.Type getReturnType() {
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
