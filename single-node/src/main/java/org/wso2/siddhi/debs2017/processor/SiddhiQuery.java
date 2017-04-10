package org.wso2.siddhi.debs2017.processor;

import com.lmax.disruptor.RingBuffer;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.debs2017.extension.DimensionAggregator;

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

    private RingBuffer<EventWrapper> buffer;
    private long sequence;


    public SiddhiQuery(RingBuffer<EventWrapper> buffer) {

        this.inStreamDefinition = "@config(async = 'true')\n" + //@config(async = 'true')@plan:async
                "define stream inStream (machine string, time string, dimension string,  uTime long,  " +
                "value double, centers int, threshold double);";

        this.query = ("" +
                "\n" +
                "from inStream " +
                "select machine, time, dimension, str:concat(machine, '-', dimension) as partitionId, uTime, value, centers, threshold " +
                "insert into inStreamA;" +
                "\n" +
                "@info(name = 'query1') partition with ( partitionId of inStreamA) " +// perform clustering
                "begin " +
                "from inStreamA#window.externalTime(uTime , 10) \n" +
                "select machine, time, dimension, singlenode:agg(value, centers, 10) as probaility, threshold " +
                " insert into detectAnomaly  " + //inner stream
//                "\n" +
//                "from #outputStream \n " +
//                "select machine, time, dimension, debs2017:markovnew(center,10) as probability, threshold, node " +
//                "insert into detectAnomaly " +
                "end;");

        this.siddhiManager = new SiddhiManager();
        this.siddhiManager.setExtension("singlenode:agg", DimensionAggregator.class);
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

                    //set the probability in debsevent
                    publishEvent(ev);

                }
            }
        });
    }


    public void publish(Event obj) {
        try {
            // inputHandler.send(obj.getData());
            inputHandler.send(obj);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }


    //setting the sequence from the ringbuffer
    public synchronized void setSequence(long l) {
        this.sequence = l;

    }


    /**
     * @param ev probability of the event sequence in the window
     *           publishing the debsevent back tot he ring buffer after setting the probability
     */
    private synchronized void publishEvent(Event ev) {
        try {
            EventWrapper wrapper = buffer.get(sequence);
            wrapper.setEvent(ev);
        } finally {
            buffer.publish(sequence);
        }


    }
}
