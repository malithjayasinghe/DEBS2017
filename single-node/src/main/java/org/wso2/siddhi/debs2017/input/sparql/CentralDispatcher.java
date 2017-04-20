package org.wso2.siddhi.debs2017.input.sparql;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.RingBuffer;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.debs2017.input.metadata.DebsMetaData;
import org.wso2.siddhi.debs2017.processor.EventWrapper;
import org.wso2.siddhi.debs2017.query.SingleNodeServer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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
public class CentralDispatcher extends DefaultConsumer {

    private static ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("%d").build();
    private static ExecutorService EXECUTOR;
    private  ArrayList<LinkedBlockingQueue<Event>> arrayList = SingleNodeServer.arraylist;
    private static final String TERMINATION_MESSAGE = "~~Termination Message~~";
    private AtomicBoolean isSparQL = SingleNodeServer.isSparQL;
    public static int count = 0;
    public static long bytesRec = 0l;

    /**
     * Dispatchers events to the disruptor after sorting
     *
     * @param channel the channel
     *
     * @param executorSize the size of the executor pool
     */
    public CentralDispatcher(Channel channel, int executorSize) {
        super(channel);
        EXECUTOR = Executors.newFixedThreadPool(executorSize, threadFactory);


    }

    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        String msg = new String(body, "UTF-8");



        if(msg.equals(TERMINATION_MESSAGE)){

            EXECUTOR.shutdown();
            try{
                EXECUTOR.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

                for(int i =0; i<arrayList.size(); i++){
                   // ObservationGroup ob = new ObservationGroup(-1l, null);
                    Event ob = new Event(-1l, null);
                    arrayList.get(i).put(ob);
                }
            } catch (Exception e){
                //do nothing
            }

        } else {
            if(isSparQL.get()){
               // System.out.println(count+"\t"+bytesRec+"\t"+body.length);
                count++;
                bytesRec += body.length;
                Runnable sparQLProcessor = new SparQLProcessor(msg, System.currentTimeMillis());
                EXECUTOR.execute(sparQLProcessor);
            } else {
                //System.out.println(count+"\t"+bytesRec+"\t"+body.length);
                count++;
                bytesRec += body.length;
//                Runnable regexProcessor = new RegexProcessor(msg, System.currentTimeMillis());
//                EXECUTOR.execute(regexProcessor);
                Runnable patternProcessor = new PatternProcessor(msg, System.currentTimeMillis());
                EXECUTOR.execute(patternProcessor);
            }

        }



    }
}
