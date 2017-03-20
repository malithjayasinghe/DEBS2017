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

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.wso2.siddhi.debs2017.input.SparQLProcessor;
import org.wso2.siddhi.debs2017.markovchain.MarkovModel;
import org.wso2.siddhi.debs2017.markovchain.MarkoveModified;

import java.util.ArrayList;

/**
 * Created by sachini on 3/9/17.
 */
public class MarkovModifiedTestCase {
    private static final Logger log = Logger.getLogger(SparQLProcessor.class);
    private ArrayList<Integer> centers = new ArrayList<>();
    private int[] center = {1, 2, 3, 1, 4, 3, 1, 3, 2, 4,1,3,2,3,4,1,4,1,2,1};
    private  int[] center1 = {1,2,2,2,2,3,2,3,2,3,2,1};
    private  double[] results1 = {0,0.0493872,0.125,1,1,0};
   // private  int[]  center = {1, 2, 3, 1, 2, 3, 1, 3, 2, 1};
    private  double[] results = {-1,1,1,1,0.25,0.25,1,0.25,0.0625,0.25,0.25,1,1,0.25,0.25,0.25,0.25,1,0.25,0.25};
    private int shift = 0;
    private int windowFull = 0;
   MarkoveModified markoveModified = new MarkoveModified();


    @Before
    public void init() {

    }

    @org.junit.Test
    public void Test1() throws InterruptedException {
        log.info("MedianAggregatorTestCase TestCase");
        for (int i = 0; i < center1.length; i++) {
            centers.add(center1[i]);

            shift++;
            if (shift > 6) {
                    markoveModified.reduceCount(centers.get(0), centers.get(1));
                    centers.remove(0);
                System.out.println(centers);
                double prob = markoveModified.updateProbability(centers);
                System.out.println(markoveModified.updateProbability(centers) + "updated probability");
                    markoveModified.execute(center1[i]);
//                Assert.assertEquals(results1[i], prob, 0.0000001);
            }
            else {
                System.out.println(centers);
                markoveModified.execute(center1[i]);
            }


        }

    }

}
