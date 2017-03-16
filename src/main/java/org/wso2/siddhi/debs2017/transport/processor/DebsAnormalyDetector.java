package org.wso2.siddhi.debs2017.transport.processor;

import com.lmax.disruptor.EventHandler;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.debs2017.Output.AlertGenerator;
import org.wso2.siddhi.debs2017.input.DebsDataPublisher;
import org.wso2.siddhi.debs2017.processor.DebsEvent;
import org.wso2.siddhi.debs2017.query.DistributedQuery;
import org.wso2.siddhi.tcp.transport.TcpNettyClient;

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
public class DebsAnormalyDetector implements EventHandler<Event> {
   // private static ArrayList<Long> arr = new ArrayList<>();
    private TcpNettyClient siddhiClient;
    static int count =0;
    static int synccount =0;

    @Override
    public void onEvent(Event event, long l, boolean b) throws Exception {

        Object [] o = event.getData();
       // addToArray(System.currentTimeMillis() - Long.parseLong(o[3].toString()));

       // System.out.println(arr.size());

        double prbability = Double.parseDouble(o[5].toString());
        if (prbability < 0.3 && prbability >= 0) {
//            System.out.println(" Machine" + o[0] + "\t" + "Timestamp" +  o[1]  +
//                    "\t" + "Dimension" + o[4] + "\t" + "Anomaly" + prbability);
//            //debsEvent.setProbThresh("0.5");
            //AlertGenerator ag = new AlertGenerator(debsEvent);
            //ag.generateAlert();

        }

        Event [] events = {event};

        System.out.println("-----"+event);
        count++;
        //System.out.println();
        //siddhiClient.send("output", events);
        send(events);
        System.out.println(count+"-------"+synccount);


//        if (arr.size() == DebsDataPublisher.superCount) {
//            long endtime = System.currentTimeMillis();
//            System.out.println("endtime" + endtime);
//            long totaltime = (endtime - DistributedQuery.starttime) / 1000;
//            System.out.println("\nTotaltime:" + (totaltime));
//            System.out.println("Throughput:" + (arr.size() / totaltime));
//            long sum = 0;
//            for (int i = 0; i < arr.size(); i++) {
//                sum = sum + arr.get(i);
//            }
//            System.out.println("Total Lat:" + (sum));
//            System.out.println("Avg Lat:" + (sum / arr.size()));
//            System.out.println("Data:" + arr.size());
//        }

    }

    private synchronized void send(Event[] events) {
        siddhiClient.send("output", events);
        synccount++;
    }

    private static synchronized void addToArray(Long diff) {
        //arr.add(diff);

    }
    public DebsAnormalyDetector(){
        this.siddhiClient = new TcpNettyClient();
        this.siddhiClient.connect("localhost", 8000);

    }
}

