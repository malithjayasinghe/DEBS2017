package org.wso2.siddhi.debs2017.processor;


import com.lmax.disruptor.RingBuffer;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;

import java.util.*;

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
public class SiddhiQuery {

    private final InputHandler inputHandler;
    private final String query;
    private final String inStreamDefinition;
    private final SiddhiManager siddhiManager;
    private ExecutionPlanRuntime executionPlanRuntime;
    private static ArrayList<Long> arr = new ArrayList<>();
    private static int count = 0;
    private RingBuffer<DebsEvent> buffer;
    private long sequence;
    private DebsEvent event;


    public SiddhiQuery(RingBuffer<DebsEvent> buffer) {
        this.query = ("" +
                "\n" +
                "from inStream " +
                "select machine, tstamp, dimension, str:concat(machine, '-', dimension) as partitionId, uTime ,value, ij_time " +
                "insert into inStreamA;" +
                "\n" +
                "@info(name = 'query1') partition with ( partitionId of inStreamA) " +// perform clustering
                "begin " +
                "from inStreamA#window.externalTime(uTime , 50) \n" +
                "select machine, tstamp, uTime, dimension, debs2017:cluster(value) as center, ij_time " +
                " insert into #outputStream; " + //inner stream
                "\n" +
                "from #outputStream#window.externalTime(uTime , 50) " +
                "select machine, tstamp, dimension, debs2017:markov(center) as probability, uTime, ij_time " +
                "insert into detectAnomaly " +
                "end;");
        this.inStreamDefinition = "@config(async = 'true')\n" + //@config(async = 'true')@plan:async
                "define stream inStream (machine string, tstamp string, uTime long, dimension string, " +
                "value double, ij_time long);";
        this.siddhiManager = new SiddhiManager();
        this.executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);
        this.buffer = buffer;
        inputHandler = executionPlanRuntime.getInputHandler("inStream");
        initCallback();
        executionPlanRuntime.start();


    }

    private void initCallback() {
        executionPlanRuntime.addCallback("detectAnomaly", new StreamCallback() {
            @Override
            public void receive(org.wso2.siddhi.core.event.Event[] events) {
                for (Event ev : events) {
                    double p = (Double) ev.getData()[3];
                    if (p != -1) {
                        //set the probability in debsevent
                        publishEvent(p);
                    } else {
                        publishEvent(-1);
                    }
                }
            }
        });
    }


    public void publish(Object[] obj) {
        try {
            inputHandler.send(obj);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    public static synchronized void addToArray(Long diff) {
        arr.add(diff);

    }

    //setting the sequence from the ringbuffer
    public synchronized void setSequence(long l) {
        this.sequence = l;

    }

    //setting the event read from the ringbuffer
    public synchronized void setEvent(DebsEvent db) {
        this.event = db;

    }


    /**
     * @param d probability of the event sequence in the window
     *          publishing the debsevent back tot he ring buffer after setting the probability
     */
    public synchronized void publishEvent(double d) {
        event = buffer.get(sequence);
        event.setProbability(d);
        buffer.publish(sequence);

    }


}
