package org.wso2.siddhi.debs2017.transport.test;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.debs2017.transport.processor.SiddhiEventHandler;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.tcp.transport.callback.StreamListener;

import java.util.ArrayList;
import java.util.concurrent.Executor;
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

    private long starttime;
    private long endtime;
    private static ArrayList<Long > arr = new ArrayList<>();
    private static ArrayList<Event> node0 = new ArrayList<>();
    private static ArrayList<Event> node1 = new ArrayList<>();
    private static ArrayList<Event> node2 = new ArrayList<>();
    private static Event currentEvent;
    private static Event[] arrivedEvents = new Event[3];


    public TestListener(StreamDefinition streamDefinition) {

        this.streamDefinition = streamDefinition;


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
        int node =(Integer)newEvent.getData()[6];
        if(node == 0)
            node0.add(newEvent);
        else if(node == 1)
            node1.add(newEvent);
        else
            node2.add(newEvent);

        if(node0.size() >0 && node1.size()>0 && node2.size()>0) {
            sortList();
        }

        /*if(arr.size()%74124==0){
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


        }*/


    }

    private synchronized void print(Event[] events) {

            arr.add(System.currentTimeMillis()-events[0].getTimestamp());
            System.out.println(events[0]);
//            if (arr.size() ==1){
//               starttime= events[0].getTimestamp();
//            }
//            if (arr.size()%74124==0) {
//                endtime = events[0].getTimestamp();
//            }


    }

    private synchronized void sortList(){
        //currentEvent = node0.get(0);
        arrivedEvents[0] = node0.get(0);
        arrivedEvents[1] = node1.get(0);
        arrivedEvents[2] = node2.get(0);
        currentEvent = arrivedEvents[0];
        int currentnode=0;
        for(int i =0; i < arrivedEvents.length; i++){
            for (int j = i+1; j<arrivedEvents.length-1;j++){
                if(getTime(currentEvent)>getTime(arrivedEvents[j])){
                    currentEvent = arrivedEvents[j];
                    currentnode = j;
                }
            }
        }

        if(currentnode == 0) {
            node0.remove(0);
        }
        else if(currentnode == 1 ) {
            node1.remove(0);
        }
        else {
            node2.remove(0);
        }
        System.out.println(currentEvent);

    }


    private synchronized int getTime(Event event){
        String time = (String)event.getData()[1];
        int timestamp = Integer.parseInt(time.substring(10));
        return timestamp;
    }
}
