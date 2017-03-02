package org.wso2.siddhi.debs2017.processor;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.wso2.siddhi.debs2017.input.DebsDataPublisher;

import java.util.ArrayList;
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
public class Main {
    public static long starttime;
    public static void main(String[] args) {

        starttime = System.currentTimeMillis();
        // Executor that will be used to construct new threads for consumers
        Executor executor = Executors.newCachedThreadPool();


        // The factory for the event
        DebsEventFactory factory = new DebsEventFactory();

        // Specify the size of the ring buffer, must be power of 2.
        int bufferSize = 1024;

        // Construct the Disruptor
        Disruptor<DebsEvent> disruptor = new Disruptor<>(DebsEvent::new, bufferSize, executor);

        DebsEventHandler lh1 = new DebsEventHandler(0,4);
        DebsEventHandler lh2 = new DebsEventHandler(1, 4);
        DebsEventHandler lh3 = new DebsEventHandler(2, 4);
        DebsEventHandler lh4 = new DebsEventHandler(3, 4);
        // Connect the handler
        disruptor.handleEventsWith(lh1, lh2,lh3, lh4);
        // disruptor.after(lh1).handleEventsWith(lh2);

        // Start the Disruptor, starts all threads running
        disruptor.start();

        // Get the ring buffer from the Disruptor to be used for publishing.
        RingBuffer<DebsEvent> ringBuffer = disruptor.getRingBuffer();

        DebsEventProducer producer = new DebsEventProducer(ringBuffer);


       // System.out.println("Starting publisher");
        //read from file
        DebsDataPublisher dp = new DebsDataPublisher("data_rdf10.csv", producer);
        dp.publish();



    }
}
