package org.wso2.siddhi.debs2017.query;

import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.wso2.siddhi.debs2017.input.DebsBenchmarkInput;
import org.wso2.siddhi.debs2017.input.rabbitmq.RabbitMQConsumer;
import org.wso2.siddhi.debs2017.input.sparql.ObservationGroup;
import org.wso2.siddhi.debs2017.input.sparql.RdfMessage;

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
public class CentralDispatcher {
    public static ArrayList<LinkedBlockingQueue<ObservationGroup>> arrayList;
    public static RingBuffer<RdfMessage> buffer;


    public static void main(String[] args) {

        if (args.length == 9) {
            String client1host = args[2];
            int client1port = Integer.parseInt(args[3]);
            String client2host = args[4];
            int client2port = Integer.parseInt(args[5]);
            String client3host = args[6];
            int client3port = Integer.parseInt(args[7]);

            int executorSize = Integer.parseInt(args[8]);

            arrayList = new ArrayList<>(executorSize);
            for (int i = 0; i < executorSize; i++) {
                arrayList.add(new LinkedBlockingQueue());
            }

           /* Disruptor<RdfMessage> disruptor = new Disruptor<>(RdfMessage::new, ringbuffersize, executor,
                    ProducerType.MULTI, new BusySpinWaitStrategy());
            buffer = disruptor.getRingBuffer();
*/

            if (args[0].equals("-hobbit")) {

                String metadata = args[1];


                try {
                    DebsBenchmarkInput db = new DebsBenchmarkInput(metadata, client1host, client1port, client2host, client2port, client3host, client3port, executorSize);
                    db.init();
                    db.run();

                } catch (Exception e) {
                    e.printStackTrace();
                }

            } else {

                String queue = args[0];
                String rmqHost = args[1];

                RabbitMQConsumer con = new RabbitMQConsumer();
                try {
                    con.consume(queue, rmqHost, client1host, client1port, client2host, client2port, client3host, client3port, executorSize);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } else {
            System.out.println("Expected 9 parameters: queue name, rmq host, client1 host, client1 port, client2 host, client2 port, client3 host, client3 port, executor size");
            System.out.println("Expected 9 parameters: -hobbit, metadataFile, client1 host, client1 port, client2 host, client2 port, client3 host, client3 port, executor size");
        }
    }
}
