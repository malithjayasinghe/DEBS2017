package org.wso2.siddhi.debs2017.input.sparql;


import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.debs2017.input.metadata.MultiNodeMetaDataQuery;

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
*/
public class SparQLProcessor extends DefaultConsumer {

    private static int value = 10;
    private static int count = 0;
    private static long starttime;
    private static ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("%d").build();
    private static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(value, threadFactory);
    public static ArrayList<LinkedBlockingQueue<Event>> arrayList = new ArrayList<>(value);

    public SparQLProcessor(Channel channel, String host1, int port1, String host2, int port2, String host3, int port3) {
        super(channel);
        starttime = System.currentTimeMillis();
        for (int i = 0; i < value; i++) {
            arrayList.add(new LinkedBlockingQueue());
        }
        Collections.synchronizedList(arrayList);
        SorterThread sort = new SorterThread(arrayList, host1, port1, host2, port2, host3, port3);
        sort.start();
        MultiNodeMetaDataQuery.run("molding_machine_10M.metadata.nt");
    }

    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        String msg = new String(body, "UTF-8");
        count++;
        if (count % 100 == 0) {
            double runTime = (System.currentTimeMillis() - starttime) / 1000;
            System.out.println("Average Throughput " + (count / runTime));
        }
        Runnable reader = new ReaderThread(msg);
        EXECUTOR.execute(reader);
    }

}