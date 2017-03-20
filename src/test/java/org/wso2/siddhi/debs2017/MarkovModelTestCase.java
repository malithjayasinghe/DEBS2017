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

package org.wso2.siddhi.debs2017;


import org.apache.jena.base.Sys;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.wso2.siddhi.debs2017.input.SparQLProcessor;
import org.wso2.siddhi.debs2017.markovchain.MarkovModel;

import java.util.ArrayList;

public class MarkovModelTestCase {
    private static final Logger log = Logger.getLogger(SparQLProcessor.class);
    private ArrayList<Integer> centers = new ArrayList<>();
    private int[] center = {1, 2, 3, 1, 4, 3, 1, 3, 2, 4,1,3,2,3,4,1,4,1,2,1};
    private  double[] results = {-1,1,1,1,0.25,0.25,1,0.25,0.0625,0.25,0.25,1,1,0.25,0.25,0.25,0.25,1,0.25,0.25};
    private int shift = 0;
    MarkovModel markovModel = new MarkovModel();


    @Before
    public void init() {

    }

    @org.junit.Test
    public void Test1() throws InterruptedException {
        log.info("MedianAggregatorTestCase TestCase");
        for (int i = 0; i < center.length; i++) {
            centers.add(center[i]);
            System.out.println(centers);
            double prob = markovModel.execute(center[i], centers);
            System.out.println(prob) ;
           Assert.assertEquals(results[i],prob,0.0);

         //  Assert.assertEquals(results[i],markovModel.execute(center[i], centers),0.001);

            shift++;
            if (shift >= 6) {
                markovModel.removeEvent(centers.get(0), centers.get(1));
                centers.remove(0);


            }


        }

    }
}
