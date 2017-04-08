package org.wso2.siddhi.debs2017.query;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import org.wso2.siddhi.debs2017.input.rabbitmq.RabbitMQConsumer;
import org.wso2.siddhi.debs2017.output.AlertGenerator;
import org.wso2.siddhi.debs2017.output.RabbitMQPublisher;
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

    public static void main(String[] args) {

        if(args.length==3){
            String inputQueue = args[0];
            String host = args[1];
            String outputQueue = args[2];
            RabbitMQPublisher rmqPublisher = new RabbitMQPublisher(outputQueue);
            AlertGenerator alertGenerator = new AlertGenerator(rmqPublisher);
            Executor executor = Executors.newCachedThreadPool();
            int buffersize = 128;
            Disruptor<EventWrapper> disruptor = new Disruptor<>(EventWrapper::new,buffersize, executor);

            RingBuffer<EventWrapper> ring = disruptor.getRingBuffer();

            SiddhiEventHandler sh1 = new SiddhiEventHandler(0L, 3L, ring);
            SiddhiEventHandler sh2 = new SiddhiEventHandler(1L, 3L, ring);
            SiddhiEventHandler sh3 = new SiddhiEventHandler(2L, 3L, ring);

            DebsAnomalyDetector debsAnormalyDetector = new DebsAnomalyDetector(alertGenerator);

            disruptor.handleEventsWith(sh1,sh2, sh3);
            disruptor.after(sh1, sh2, sh3).handleEventsWith(debsAnormalyDetector);
            // disruptor.handleEventsWith(debsAnormalyDetector);

            disruptor.start();
            RabbitMQConsumer rmq = new RabbitMQConsumer();
            rmq.consume(inputQueue, host, ring);
        } else {
            System.out.println("Expected 3 parameters: inputqueue, host, outputqueue");
        }


    }
}
