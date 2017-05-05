package org.wso2.siddhi.debs2017.query;

import org.wso2.siddhi.debs2017.input.DebsBenchmarkInput;
import org.wso2.siddhi.debs2017.input.rabbitmq.RabbitMQConsumer;
import org.wso2.siddhi.debs2017.transport.utils.TcpNettyClient;

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

    public static TcpNettyClient siddhiClient0 = new TcpNettyClient();
    public static TcpNettyClient siddhiClient1 = new TcpNettyClient();


    public static void main(String[] args) {

        if (args.length == 6) {
            String client1host = args[2];
            int client1port = Integer.parseInt(args[3]);
            String client2host = args[4];
            int client2port = Integer.parseInt(args[5]);

            siddhiClient0.connect(client1host, client1port);
            siddhiClient1.connect(client2host, client2port);


           /* Disruptor<RdfMessage> disruptor = new Disruptor<>(RdfMessage::new, ringbuffersize, executor,
                    ProducerType.MULTI, new BusySpinWaitStrategy());
            buffer = disruptor.getRingBuffer();
*/

            if (args[0].equals("-hobbit")) {

                String metadata = args[1];


                try {
                    DebsBenchmarkInput db = new DebsBenchmarkInput(metadata);
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
                    con.consume(queue, rmqHost);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } else {
            System.out.println("Expected 9 parameters: queue name, rmq host, client1 host, client1 port, client2 host, client2 port ");
            System.out.println("Expected 9 parameters: -hobbit, metadataFile, client1 host, client1 port, client2 host, client2 port ");
        }
    }
}
