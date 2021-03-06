package org.wso2.siddhi.debs2017;

import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;

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
public class MetaDataMergerTestCase {

    @org.junit.Test
    public void Test1(){

        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "@config(async = 'true') \n" +
                "define stream inStream (machine string, tstamp string, uTime long, dimension string, value1 double, value2 int);";
        String query = ("" +
                "\n" +
                "from inStream\n" +
                "select machine, tstamp, dimension, uTime, str:concat(machine, '-', dimension) as partitionId, value1, value2 \n" +
                "insert into inStreamA;\n" +
                "@info(name = 'query1') partition with ( partitionId of inStreamA) \n" +
                "begin " +
                "from inStreamA#window.externalTime(uTime , 10) \n" +
                "select machine, tstamp, uTime, dimension, debs2017:cluster(value1, value2) as center " +
                " insert into outputStream; " + //inner stream
                "end");

        System.out.println(inStreamDefinition + query);
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(org.wso2.siddhi.core.event.Event[] events) {

                for(Event ev : events){
                    System.out.println(ev.getData()[0]+"\t"+ ev.getData()[1]+"\t"+ ev.getData()[2]+"\t"+ ev.getData()[3]+"\n");


                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inStream");

        try {


            inputHandler.send(new Object[]{"m1","t1",1485859203100L,"d1", 0.0, 13});
            inputHandler.send(new Object[]{"m1","t1",1485859203101L,"d1", 1.0, 13});
            inputHandler.send(new Object[]{"m1","t1",1485859203102L,"d1", 2.0, 13});
            inputHandler.send(new Object[]{"m1","t1",1485859203103L,"d1", 3.0, 13});
            inputHandler.send(new Object[]{"m1","t1",1485859203104L,"d1", 4.0, 13});
            inputHandler.send(new Object[]{"m1","t1",1485859203105L,"d1", 5.0, 13});
            inputHandler.send(new Object[]{"m1","t1",1485859203106L,"d1", 6.0, 13});
            inputHandler.send(new Object[]{"m1","t1",1485859203107L,"d1", 7.0, 13});
            inputHandler.send(new Object[]{"m1","t1",1485859203108L,"d1", 8.0, 13});
            inputHandler.send(new Object[]{"m1","t1",1485859203109L,"d1", 9.0, 13});
            inputHandler.send(new Object[]{"m1","t1",1485859203110L,"d1", 10.0, 13});
            inputHandler.send(new Object[]{"m1","t1",1485859203111L,"d1", 11.0, 13});
            inputHandler.send(new Object[]{"m1","t1",1485859203112L,"d1", 12.0, 13});
            inputHandler.send(new Object[]{"m1","t1",1485859203113L,"d1", 13.0, 13});
            inputHandler.send(new Object[]{"m1","t1",1485859203114L,"d1", 14.0, 13});
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        executionPlanRuntime.start();



    }

    @org.junit.Test
    public void Test2(){

        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "@config(async = 'true') \n" +
                "define stream inStream (machine string, tstamp string, uTime long, dimension string, value1 double, value2 int);";
        String query = ("" +
                "\n" +
                "from inStream\n" +
                "select machine, tstamp, dimension, uTime, str:concat(machine, '-', dimension) as partitionId, value1, value2 \n" +
                "insert into inStreamA;\n" +
                "@info(name = 'query1') partition with ( partitionId of inStreamA) \n" +
                "begin " +
                "from inStreamA#window.externalTime(uTime , 10) \n" +
                "select machine, tstamp, uTime, dimension, debs2017:cluster(value1, value2) as center " +
                " insert into outputStream; " + //inner stream
                "end");

        System.out.println(inStreamDefinition + query);
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(org.wso2.siddhi.core.event.Event[] events) {

                for(Event ev : events){
                    System.out.println(ev.getData()[0]+"\t"+ ev.getData()[1]+"\t"+ ev.getData()[2]+"\t"+ ev.getData()[3]+"\n");


                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inStream");

        try {


            inputHandler.send(new Object[]{"m1","t1",1485859203100L,"d1", 0.0, 13});
            inputHandler.send(new Object[]{"m1","t1",1485859203101L,"d1", 1.0, 13});
            inputHandler.send(new Object[]{"m1","t1",1485859203102L,"d1", 2.0, 13});
            inputHandler.send(new Object[]{"m1","t1",1485859203103L,"d1", 3.0, 13});
            inputHandler.send(new Object[]{"m1","t1",1485859203104L,"d1", 4.0, 13});
            inputHandler.send(new Object[]{"m1","t1",1485859203105L,"d1", 5.0, 13});
            inputHandler.send(new Object[]{"m1","t1",1485859203106L,"d1", 6.0, 13});
            inputHandler.send(new Object[]{"m1","t1",1485859203107L,"d1", 1.0, 13});
            inputHandler.send(new Object[]{"m1","t1",1485859203108L,"d1", 2.0, 13});
            inputHandler.send(new Object[]{"m1","t1",1485859203109L,"d1", 3.0, 13});
            inputHandler.send(new Object[]{"m1","t1",1485859203110L,"d1", 4.0, 13});
            inputHandler.send(new Object[]{"m1","t1",1485859203111L,"d1", 5.0, 13});
            inputHandler.send(new Object[]{"m1","t1",1485859203112L,"d1", 6.0, 13});
            inputHandler.send(new Object[]{"m1","t1",1485859203113L,"d1", 7.0, 13});
            inputHandler.send(new Object[]{"m1","t1",1485859203114L,"d1", 8.0, 13});
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        executionPlanRuntime.start();










    }








}
