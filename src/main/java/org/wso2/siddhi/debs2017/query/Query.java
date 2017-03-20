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

package org.wso2.siddhi.debs2017.query;

import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.debs2017.Output.AlertGenerator;
import org.wso2.siddhi.debs2017.input.DataPublisher;

import java.util.ArrayList;


public class Query {

    ArrayList<Long> latency = new ArrayList<>();
    private Query() {
    }

    /**
     * The main method
     *
     * @param args arguments
     */
    public static void main(String[] args) {

        Query query = new Query();
        query.run();
    }

    /**
     * Starts the threads related to Query
     */
    public void run() {

        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "@config(async = 'true') " +
                " define stream inStream (machine string, tstamp string, uTime long, dimension string, addedTime long, " +
                "value double);";

        String query = ("" +
                "\n" +
                "from inStream " +
                "select machine, tstamp, dimension, str:concat(machine, '-', dimension) as partitionId, uTime ,value, addedTime " +
                "insert into inStreamA;" +
                "\n" +
                "@info(name = 'query1') partition with ( partitionId of inStreamA) " +// perform clustering
                "begin " +
                "from inStreamA#window.externalTime(uTime , 100) \n" +
                "select machine, tstamp, uTime, dimension, debs2017:cluster(value) as center, addedTime " +
                " insert into #outputStream; " + //inner stream
                "\n" +
                "from #outputStream " +
                "select machine, tstamp, dimension, debs2017:markovnew(center) as probability, addedTime " +
                "insert into detectAnomaly " +
                "end;");


        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);
        executionPlanRuntime.addCallback("detectAnomaly", new StreamCallback() {
            @Override
            public void receive(org.wso2.siddhi.core.event.Event[] events) {
                for (Event ev : events) {
                 // long diff =   System.currentTimeMillis() - (long)ev.getData()[4];
                  //  latency.add(diff);
                  System.out.println(ev.getData()[0] + "," + ev.getData()[1] + "," + ev.getData()[2] + "," + ev.getData()[3]);
                   // System.out.println("Events added"+ " "+ DataPublisher.count +" "+ "Supercount" + " " + DataPublisher.supercount);
                   // System.out.println(latency.size() + " " + "events processed");

                }


            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inStream");
        DataPublisher dataPublisher = new DataPublisher("rdfData_extract_100m_time.csv", inputHandler);
        executionPlanRuntime.start();
        dataPublisher.publish();

        if(DataPublisher.supercount == latency.size()){
            System.out.println("Data processed");
            long endtime = System.currentTimeMillis();
            System.out.println("endtime" + endtime);
            long totaltime = (endtime - DataPublisher.startime) / 1000;
            System.out.println("\nTotaltime:" + (totaltime));
            System.out.println("Throughput:" + (latency.size() / totaltime));
            double sum = 0;
            for (int i = 0; i < latency.size(); i++) {
                sum = sum + latency.get(i);
            }
            System.out.println("Total Lat:" + (sum));
            System.out.println("Avg Lat:" + (sum / latency.size()));
            System.out.println("Data:" + latency.size());
        }

        while (true) {
            try {
                Thread.currentThread().sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


    }

}
