package org.wso2.siddhi.debs2017.transport;

import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.debs2017.output.RabbitMQPublisher;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.tcp.transport.callback.StreamListener;

import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;

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
public class OutputListener implements StreamListener {

    private StreamDefinition streamDefinition;
    private int currentnode = 0;
    private long starttime;
    private long endtime;
    static int count = 0;
    private ArrayList<Long> arr = new ArrayList<>();
    private static LinkedBlockingQueue<Event>[] blockingQueues = new LinkedBlockingQueue[3];

    /**
     * The constructor
     *
     * @param streamDefinition the definition of the stream
     * @param rmq the rabitmq queue to publish the data
     */
    public OutputListener(StreamDefinition streamDefinition, RabbitMQPublisher rmq) {
        this.streamDefinition = streamDefinition;
        SortingThread sorter = new SortingThread(rmq, blockingQueues);
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
        Event newEvent = events[0];
        int node = (Integer) newEvent.getData()[5];
        if (node == 0)
            try {
                blockingQueues[0].put(newEvent);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        else if (node == 1)
            try {
                blockingQueues[1].put(newEvent);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        else
            try {
                blockingQueues[2].put(newEvent);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
    }

    /**
     * prints the output
     *
     * @param events the events
     */
    private synchronized void print(Event[] events) {

        count++;
        System.out.println(count + "\t" + events[0]);
        arr.add(System.currentTimeMillis() - events[0].getTimestamp());
        if (arr.size() == 1) {
            starttime = events[0].getTimestamp();
        }
        if (arr.size() % 100 == 0) {
            endtime = events[0].getTimestamp();
        }

    }


}
