package org.wso2.siddhi.debs2017.query;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.EventHandlerGroup;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import org.hobbit.core.data.RabbitQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.debs2017.input.DebsBenchmarkSystem;
import org.wso2.siddhi.debs2017.input.rabbitmq.RabbitMQConsumer;
import org.wso2.siddhi.debs2017.output.AlertGenerator;
import org.wso2.siddhi.debs2017.processor.DebsAnomalyDetector;
import org.wso2.siddhi.debs2017.processor.EventWrapper;
import org.wso2.siddhi.debs2017.processor.SiddhiEventHandler;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

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
public class DistributedQuery {

    private static final Logger logger = LoggerFactory.getLogger(DistributedQuery.class);

    /**
     * 1 : input queue : -hobbit
     * 2 : output queue : metedataFilename
     * 3 : executor size
     * 4 : ring buffersize
     * 5 : no of handlers --@TODO
     */
    public static void main(String[] args) {
        if (args.length == 5) {
            int executorsize = Integer.parseInt(args[2]);
            int ringbuffersize = Integer.parseInt(args[3]);
            if (args[0].equals("-hobbit")) {

                String metadata = args[1];
                RabbitQueue rmqPublisher = null;
                AlertGenerator alertGenerator = null;
                Executor executor = Executors.newCachedThreadPool();
                int buffersize = ringbuffersize;
                Disruptor<EventWrapper> disruptor = new Disruptor<>(EventWrapper::new, buffersize, executor);
                RingBuffer<EventWrapper> ring = disruptor.getRingBuffer();

                SiddhiEventHandler sh1 = new SiddhiEventHandler(0L, 3L, ring);
                SiddhiEventHandler sh2 = new SiddhiEventHandler(1L, 3L, ring);
                SiddhiEventHandler sh3 = new SiddhiEventHandler(2L, 3L, ring);

                DebsAnomalyDetector debsAnormalyDetector = null;
                disruptor.handleEventsWith(sh1, sh2, sh3);

                logger.debug("Running...");
                DebsBenchmarkSystem system = null;
                try {
                    system = new DebsBenchmarkSystem(ring, metadata, executorsize);
                    system.init();
                    //disruptor
                    rmqPublisher = system.getOutputQueue();
                    alertGenerator = new AlertGenerator(rmqPublisher);
                    debsAnormalyDetector = new DebsAnomalyDetector(alertGenerator);
                    disruptor.after(sh1, sh2, sh3).handleEventsWith(debsAnormalyDetector);
                    disruptor.start();
                    system.run();
                } finally {
                    if (system != null) {
                        system.close();
                    }
                }
                logger.debug("Finished.");
            } else {

                String inputQueue = args[0];
                String outputQueue = args[1];
                RabbitQueue output = null;
                ConnectionFactory factory = new ConnectionFactory();
                factory.setHost("127.0.0.1");
                Channel channel = null;

                try {
                    channel = factory.newConnection().createChannel();
                    channel.basicQos(1);
                    channel.queueDeclare(outputQueue, false, false, true, null);
                } catch (Exception e) {
                    e.printStackTrace();
                }


                Executor executor = Executors.newCachedThreadPool();
                int buffersize = ringbuffersize;
                Disruptor<EventWrapper> disruptor = new Disruptor<>(EventWrapper::new, buffersize, executor);

                RingBuffer<EventWrapper> ring = disruptor.getRingBuffer();

                SiddhiEventHandler sh1 = new SiddhiEventHandler(0L, 3L, ring);
                SiddhiEventHandler sh2 = new SiddhiEventHandler(1L, 3L, ring);
                SiddhiEventHandler sh3 = new SiddhiEventHandler(2L, 3L, ring);
                output = new RabbitQueue(channel, outputQueue);
                AlertGenerator alertGenerator = new AlertGenerator(output);
                DebsAnomalyDetector debsAnormalyDetector = new DebsAnomalyDetector(alertGenerator);
                disruptor.handleEventsWith(sh1, sh2, sh3);
                disruptor.after(sh1, sh2, sh3).handleEventsWith(debsAnormalyDetector);
                disruptor.start();
                RabbitMQConsumer rmq = new RabbitMQConsumer();
                rmq.consume(inputQueue, ring, executorsize);

            }

        }
    }
}
