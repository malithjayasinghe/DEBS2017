package org.wso2.siddhi.debs2017;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.wso2.siddhi.debs2017.input.SparQLProcessor;
import org.wso2.siddhi.debs2017.kmeans.Clusterer;

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
public class ClustererTestCase {

    private static final Logger log = Logger.getLogger(Clusterer.class);

    @org.junit.Test
    public void Test1() throws InterruptedException {

        log.info("ClustererTestCase TestCase 01");

        ArrayList<Double> input = new ArrayList<>();
        input.add(2.0);
        input.add(10.0);
        input.add(8.0);
        input.add(1.0);
        input.add(4.0);
        input.add(3.0);
        //input.add(10.0);


        Clusterer cl = new Clusterer(3, 10, input);
        cl.cluster();
        Assert.assertEquals(cl.getCenter(10.0), 3);



    }

    @org.junit.Test
    public void Test2() throws InterruptedException {

        log.info("ClustererTestCase TestCase 02");

        ArrayList<Double> input = new ArrayList<>();
        input.add(1.0);
        input.add(3.0);
        input.add(2.0);
        input.add(4.0);
        input.add(5.0);

        Clusterer test =new Clusterer(2, 10, input);
        test.cluster();
        Assert.assertEquals(test.getCenter(6.0), 2);



    }

    @org.junit.Test
    public void Test3() throws InterruptedException {

        log.info("ClustererTestCase TestCase 03");

        ArrayList<Double> input = new ArrayList<>();
        input.add(7.0);
        //input.add(8.0);
        Clusterer test =new Clusterer(3, 10, input);
        test.cluster();
        Assert.assertEquals(test.getCenter(8.0), 1);



    }





    @org.junit.Test
    public void Test4() throws InterruptedException {
        log.info("ClustererTestCase TestCase 04");
        ArrayList<Double> input = new ArrayList<>();
        //input.add(7.0);
        //input.add(8.0);
        Clusterer test =new Clusterer(3, 10, input);
        // log.info("Test case 1 result : "+test.getCenter());
        // System.out.println(test.getCenter(8.0));
        test.cluster();
        Assert.assertEquals(test.getCenter(8.0), 1);


    }
}

