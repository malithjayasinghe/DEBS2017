package org.wso2.siddhi.debs2017.transport.test;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.debs2017.Output.RabbitMQPublisher;
import org.wso2.siddhi.debs2017.transport.SortingThread;
import org.wso2.siddhi.debs2017.transport.processor.SiddhiEventHandler;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.tcp.transport.callback.StreamListener;

import java.util.ArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

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
public class TestListener implements StreamListener {

    private StreamDefinition streamDefinition;
    private int currentnode=0;

    private long starttime;
    private long endtime;
    public static  LinkedBlockingQueue<Event> lbqueue0 = new LinkedBlockingQueue<Event>();
    public static LinkedBlockingQueue<Event> lbqueue1 = new LinkedBlockingQueue<Event>();
    public static LinkedBlockingQueue<Event> lbqueue2 = new LinkedBlockingQueue<Event>();
    static int count =0;

    private ArrayList<Long> arr = new ArrayList<>();

    Event currentEvent;

    public TestListener(StreamDefinition streamDefinition, RabbitMQPublisher rmq) {

        this.streamDefinition = streamDefinition;
        SortingThread sorter = new SortingThread(rmq);
        sorter.start();


    }

    @Override
    public StreamDefinition getStreamDefinition() {
        return streamDefinition;
    }


    @Override
    public void onEvent(Event event) {

    }

    @Override
    public void onEvents(Event[] events) {
     // print(events);
           Event newEvent = events[0];
           int node =(Integer)newEvent.getData()[5];
                if(node == 0)
                    try {
                        lbqueue0.put(newEvent);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                else if(node == 1)
                    try {
                        lbqueue1.put(newEvent);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                else
                    try {
                        lbqueue2.put(newEvent);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }



       /* if(arr.size()%100==0){
            System.out.println("Starttime "+starttime);
            System.out.println("Endtime "+endtime);
            System.out.println("Running in ms "+(endtime-starttime));
            System.out.println("Throughput "+arr.size()/((endtime-starttime)/1000));
            double sum =0;
            for (int i =0; i<arr.size(); i++){
                sum +=arr.get(i);
            }
            System.out.println("Total Latency"+sum);
            System.out.println("Average Latency"+(sum/arr.size()));
            System.out.println("---------------------------------------------");


        }
*/

    }

    private synchronized void print(Event[] events) {

            count++;
            System.out.println(count+"\t"+events[0]);
            arr.add(System.currentTimeMillis()-events[0].getTimestamp());

            if (arr.size() ==1){
               starttime= events[0].getTimestamp();
            }
            if (arr.size()%100==0) {
                endtime = events[0].getTimestamp();
            }


    }


}
