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
        print(events);
        if(arr.size()%74124==0){
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


        }


    }

    private synchronized void print(Event[] events) {

            arr.add(System.currentTimeMillis()-events[0].getTimestamp());
            //System.out.println(events[0]);
            if (arr.size() ==1){
               starttime= events[0].getTimestamp();
            }
            if (arr.size()%74124==0) {
                endtime = events[0].getTimestamp();
            }


    }
}
