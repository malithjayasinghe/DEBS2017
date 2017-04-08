package org.wso2.siddhi.debs2017.input.rabbitmq;

import com.lmax.disruptor.RingBuffer;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import org.wso2.siddhi.debs2017.input.sparql.SparQLProcessor;
import org.wso2.siddhi.debs2017.processor.EventWrapper;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

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
public class RabbitMQConsumer {

    private static String TASK_QUEUE_NAME = "";
    static ExecutorService executors = Executors.newFixedThreadPool(8);

    /**
     *  Consumes the sample data
     *
     */
    public void consume(String queue, RingBuffer<EventWrapper> ringBuffer) {
        TASK_QUEUE_NAME = queue;
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("127.0.0.1");
        final Connection connection;
        final Channel channel;
        final Consumer consumer;
        try {
            connection = factory.newConnection(executors);
            channel = connection.createChannel();
            consumer = new SparQLProcessor(channel, ringBuffer);
            boolean autoAck = true; // acknowledgment is covered below
            channel.basicConsume(TASK_QUEUE_NAME, autoAck, consumer);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }



}
