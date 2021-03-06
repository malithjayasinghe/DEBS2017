package org.wso2.siddhi.debs2017.input.sparql;


import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.debs2017.input.metadata.DebsMetaData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;

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
*
* Publishers spaql jobs to executor thread pool + start the sorting threads which publishes events to the back-end workers
*/
public class EventDispatcher extends DefaultConsumer {


    private static int count = 0;
    private static long startTime;
    private static ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("%d").build();
    private static ExecutorService EXECUTOR ;
    public static ArrayList<LinkedBlockingQueue<Event>> arrayList;

    /**
     * The constructor
     *
     * @param channel the channel
     * @param host1 the host 1
     * @param port1 the port 1
     * @param host2 the host 2
     * @param port2 the port 2
     * @param host3 the host 3
     * @param port3 the port 3
     */
    public EventDispatcher(Channel channel, String host1, int port1, String host2, int port2, String host3, int port3, int executorSize) {
        super(channel);
        EXECUTOR = Executors.newFixedThreadPool(executorSize, threadFactory);
        arrayList = new ArrayList<>(executorSize);
        startTime = System.currentTimeMillis();
        for (int i = 0; i < executorSize; i++) {
            arrayList.add(new LinkedBlockingQueue());
        }

        //TODO: I do not think we need to synchronize the array list
        Collections.synchronizedList(arrayList);
        SorterThread sort = new SorterThread(arrayList, host1, port1, host2, port2, host3, port3);
        sort.start();
        DebsMetaData.generate("molding_machine_10M.metadata.nt");
    }

    /**
     * Handle delivery
     *
     * @param consumerTag the consumer tag
     * @param envelope the envelop
     * @param properties the properties
     * @param body the body
     * @throws IOException thrown if an IO exception occurs
     */
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        String msg = new String(body, "UTF-8");
        count++;
        if (count % 33500 == 0) {
            double runTime = (System.currentTimeMillis() - startTime) / 1000;
            System.out.println("Average Throughput " + (count / runTime));
        }
        Runnable reader = new SPARQLProcessor(msg);
        EXECUTOR.execute(reader);
    }

}