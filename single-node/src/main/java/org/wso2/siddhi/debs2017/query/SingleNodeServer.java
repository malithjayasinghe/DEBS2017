package org.wso2.siddhi.debs2017.query;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import org.hobbit.core.data.RabbitQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.debs2017.input.DebsBenchmarkSystem;
import org.wso2.siddhi.debs2017.input.metadata.DebsMetaData;

import org.wso2.siddhi.debs2017.input.sparql.SorterThread;
import org.wso2.siddhi.debs2017.output.AlertGenerator;
import org.wso2.siddhi.debs2017.processor.DebsAnomalyDetector;
import org.wso2.siddhi.debs2017.processor.EventWrapper;
import org.wso2.siddhi.debs2017.processor.SiddhiEventHandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
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
public class SingleNodeServer {

    private static final Logger logger = LoggerFactory.getLogger(SingleNodeServer.class);

    public static ArrayList<LinkedBlockingQueue<Event>> arrayList;

    /**
     * 1 : input queue : -hobbit
     * 2 : output queue : metedataFilename
     * 3 : executor size
     * 4 : ring buffersize
     * 5 : no of handlers --@TODO
     */

    public static void createhandler(int handlersize, RingBuffer ring, DebsAnomalyDetector debsAnormalyDetector, Disruptor disruptor) {
        switch (handlersize) {
            case 2: {

                SiddhiEventHandler sh1 = new SiddhiEventHandler(0L, 2L, ring);
                SiddhiEventHandler sh2 = new SiddhiEventHandler(1L, 2L, ring);
                disruptor.handleEventsWith(sh1, sh2);
                disruptor.after(sh1, sh2).handleEventsWith(debsAnormalyDetector);
                disruptor.start();
                break;

            }
            case 3: {
                SiddhiEventHandler sh1 = new SiddhiEventHandler(0L, 3L, ring);
                SiddhiEventHandler sh2 = new SiddhiEventHandler(1L, 3L, ring);
                SiddhiEventHandler sh3 = new SiddhiEventHandler(2L, 3L, ring);
                disruptor.handleEventsWith(sh1, sh2, sh3);
                disruptor.after(sh1, sh2, sh3).handleEventsWith(debsAnormalyDetector);
                disruptor.start();
                break;

            }
            case 4: {
                SiddhiEventHandler sh1 = new SiddhiEventHandler(0L, 4L, ring);
                SiddhiEventHandler sh2 = new SiddhiEventHandler(1L, 4L, ring);
                SiddhiEventHandler sh3 = new SiddhiEventHandler(2L, 4L, ring);
                SiddhiEventHandler sh4 = new SiddhiEventHandler(3L, 4L, ring);
                disruptor.handleEventsWith(sh1, sh2, sh3, sh4);
                disruptor.after(sh1, sh2, sh3, sh4).handleEventsWith(debsAnormalyDetector);
                disruptor.start();
                break;

            }
            case 5: {
                SiddhiEventHandler sh1 = new SiddhiEventHandler(0L, 5L, ring);
                SiddhiEventHandler sh2 = new SiddhiEventHandler(1L, 5L, ring);
                SiddhiEventHandler sh3 = new SiddhiEventHandler(2L, 5L, ring);
                SiddhiEventHandler sh4 = new SiddhiEventHandler(3L, 5L, ring);
                SiddhiEventHandler sh5 = new SiddhiEventHandler(4L, 5L, ring);
                disruptor.handleEventsWith(sh1, sh2, sh3, sh4, sh5);
                disruptor.after(sh1, sh2, sh3, sh4, sh5).handleEventsWith(debsAnormalyDetector);
                disruptor.start();
                break;

            }
            case 6: {
                SiddhiEventHandler sh1 = new SiddhiEventHandler(0L, 6L, ring);
                SiddhiEventHandler sh2 = new SiddhiEventHandler(1L, 6L, ring);
                SiddhiEventHandler sh3 = new SiddhiEventHandler(2L, 6L, ring);
                SiddhiEventHandler sh4 = new SiddhiEventHandler(3L, 6L, ring);
                SiddhiEventHandler sh5 = new SiddhiEventHandler(4L, 6L, ring);
                SiddhiEventHandler sh6 = new SiddhiEventHandler(5L, 6L, ring);
                disruptor.handleEventsWith(sh1, sh2, sh3, sh4, sh5, sh6);
                disruptor.after(sh1, sh2, sh3, sh4, sh5, sh6).handleEventsWith(debsAnormalyDetector);
                disruptor.start();
                break;
            }
            case 7: {
                SiddhiEventHandler sh1 = new SiddhiEventHandler(0L, 7L, ring);
                SiddhiEventHandler sh2 = new SiddhiEventHandler(1L, 7L, ring);
                SiddhiEventHandler sh3 = new SiddhiEventHandler(2L, 7L, ring);
                SiddhiEventHandler sh4 = new SiddhiEventHandler(3L, 7L, ring);
                SiddhiEventHandler sh5 = new SiddhiEventHandler(4L, 7L, ring);
                SiddhiEventHandler sh6 = new SiddhiEventHandler(5L, 7L, ring);
                SiddhiEventHandler sh7 = new SiddhiEventHandler(6L, 7L, ring);
                disruptor.handleEventsWith(sh1, sh2, sh3, sh4, sh5, sh6, sh7);
                disruptor.after(sh1, sh2, sh3, sh4, sh5, sh6, sh7).handleEventsWith(debsAnormalyDetector);
                disruptor.start();
                break;
            }
            case 8: {
                SiddhiEventHandler sh1 = new SiddhiEventHandler(0L, 8L, ring);
                SiddhiEventHandler sh2 = new SiddhiEventHandler(1L, 8L, ring);
                SiddhiEventHandler sh3 = new SiddhiEventHandler(2L, 8L, ring);
                SiddhiEventHandler sh4 = new SiddhiEventHandler(3L, 8L, ring);
                SiddhiEventHandler sh5 = new SiddhiEventHandler(4L, 8L, ring);
                SiddhiEventHandler sh6 = new SiddhiEventHandler(5L, 8L, ring);
                SiddhiEventHandler sh7 = new SiddhiEventHandler(6L, 8L, ring);
                SiddhiEventHandler sh8 = new SiddhiEventHandler(7L, 8L, ring);
                disruptor.handleEventsWith(sh1, sh2, sh3, sh4, sh5, sh6, sh7, sh8);
                disruptor.after(sh1, sh2, sh3, sh4, sh5, sh6, sh7, sh8).handleEventsWith(debsAnormalyDetector);
                disruptor.start();
                break;
            }
            case 9: {
                SiddhiEventHandler sh1 = new SiddhiEventHandler(0L, 9L, ring);
                SiddhiEventHandler sh2 = new SiddhiEventHandler(1L, 9L, ring);
                SiddhiEventHandler sh3 = new SiddhiEventHandler(2L, 9L, ring);
                SiddhiEventHandler sh4 = new SiddhiEventHandler(3L, 8L, ring);
                SiddhiEventHandler sh5 = new SiddhiEventHandler(4L, 9L, ring);
                SiddhiEventHandler sh6 = new SiddhiEventHandler(5L, 9L, ring);
                SiddhiEventHandler sh7 = new SiddhiEventHandler(6L, 9L, ring);
                SiddhiEventHandler sh8 = new SiddhiEventHandler(7L, 9L, ring);
                SiddhiEventHandler sh9 = new SiddhiEventHandler(8L, 9L, ring);
                disruptor.handleEventsWith(sh1, sh2, sh3, sh4, sh5, sh6, sh7, sh8, sh9);
                disruptor.after(sh1, sh2, sh3, sh4, sh5, sh6, sh7, sh8, sh9).handleEventsWith(debsAnormalyDetector);
                disruptor.start();
                break;
            }
            case 10: {
                SiddhiEventHandler sh1 = new SiddhiEventHandler(0L, 10L, ring);
                SiddhiEventHandler sh2 = new SiddhiEventHandler(1L, 10L, ring);
                SiddhiEventHandler sh3 = new SiddhiEventHandler(2L, 10L, ring);
                SiddhiEventHandler sh4 = new SiddhiEventHandler(3L, 10L, ring);
                SiddhiEventHandler sh5 = new SiddhiEventHandler(4L, 10L, ring);
                SiddhiEventHandler sh6 = new SiddhiEventHandler(5L, 10L, ring);
                SiddhiEventHandler sh7 = new SiddhiEventHandler(6L, 10L, ring);
                SiddhiEventHandler sh8 = new SiddhiEventHandler(7L, 10L, ring);
                SiddhiEventHandler sh9 = new SiddhiEventHandler(8L, 10L, ring);
                SiddhiEventHandler sh10 = new SiddhiEventHandler(9L, 10L, ring);
                disruptor.handleEventsWith(sh1, sh2, sh3, sh4, sh5, sh6, sh7, sh8, sh9, sh10);
                disruptor.after(sh1, sh2, sh3, sh4, sh5, sh6, sh7, sh8, sh9, sh10).handleEventsWith(debsAnormalyDetector);
                disruptor.start();
                break;
            }

        }
    }

    public static void main(String[] args) {
        int handlers = Integer.parseInt(args[4]);
        if (args.length == 5) {
            int executorSize = Integer.parseInt(args[2]);
            int ringBufferSize = Integer.parseInt(args[3]);

            arrayList = new ArrayList<>(executorSize);
            for (int i = 0; i < executorSize; i++) {
                arrayList.add(new LinkedBlockingQueue<Event>());
            }

            RabbitQueue rmqPublisher;
            AlertGenerator alertGenerator;
            Executor executor = Executors.newCachedThreadPool();
            int bufferSize = ringBufferSize;
            Disruptor<EventWrapper> disruptor = new Disruptor<>(EventWrapper::new, bufferSize, executor);
            RingBuffer<EventWrapper> ring = disruptor.getRingBuffer();
            DebsAnomalyDetector debsAnormalyDetector;
            SorterThread sort = new SorterThread(arrayList, ring);
            sort.start();

            if (args[0].equals("-hobbit")) {

                String metadata = args[1];

                DebsMetaData.load(metadata);

                logger.debug("Running...");

                try (DebsBenchmarkSystem system = new DebsBenchmarkSystem(executorSize, arrayList)){
                    system.init();
                    rmqPublisher = system.getOutputQueue();
                    alertGenerator = new AlertGenerator(rmqPublisher);
                    debsAnormalyDetector = new DebsAnomalyDetector(alertGenerator);
                    createhandler(handlers, ring, debsAnormalyDetector, disruptor);
                    system.run();
                }
                logger.debug("Finished.");
            } else {
                String inputQueue = args[0];
                String outputQueue = args[1];
                try (DebsBenchmarkSystem system = new DebsBenchmarkSystem(inputQueue, outputQueue, executorSize, arrayList)){
                    rmqPublisher = system.getOutputQueue();
                    alertGenerator = new AlertGenerator(rmqPublisher);
                    debsAnormalyDetector = new DebsAnomalyDetector(alertGenerator);
                    createhandler(handlers, ring, debsAnormalyDetector, disruptor);
                }catch (Exception e)
                {
                    e.printStackTrace();
                }

            }

        }
    }


}
