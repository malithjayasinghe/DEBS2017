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

import org.junit.Assert;
import org.wso2.siddhi.debs2017.markovchain.Markovnew;

import java.util.ArrayList;


public class MarkoveNewTestCase {
    Markovnew markovnew = new Markovnew();
    //private int[] center = {1,2,2,2,2,3,2,3,2,3,2,1};
    private int[] center = {1,2,2,2,2,3,2,3,2,3,2};

    private  double[] results = {0.10546875, 0.10546875, 0.03456, 0.03456,0.015625,0.015625};

    private ArrayList<Integer> centers = new ArrayList<>();
    int shift = 0;
    double prob;

    @org.junit.Test
    public void Test1() throws InterruptedException {

        for (int i = 0; i < center.length; i++) {
            centers.add(center[i]);

            System.out.println(centers);
            if(centers.size() >5) {
                markovnew.execute(centers);
                prob = markovnew.updateProbability(centers);
                System.out.println(prob);

                Assert.assertEquals(results[i-5],prob,0.0);

            }



        }


    }

}
