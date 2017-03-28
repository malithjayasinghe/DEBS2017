package org.wso2.siddhi.debs2017.query;

import org.wso2.siddhi.debs2017.input.rabbitmq.RabbitMQConsumer;

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

    public static void main(String[] args) {

        if(args.length==8){
            String queue = args [0];
            String rmqHost = args [1];
            String client1host = args [2];
            int client1port = Integer.parseInt(args[3]);
            String client2host = args [4];
            int client2port = Integer.parseInt(args[5]);
            String client3host = args [6];
            int client3port = Integer.parseInt(args[7]);

            RabbitMQConsumer con = new RabbitMQConsumer();
            try {

                con.consume(queue, rmqHost, client1host, client1port, client2host, client2port, client3host, client3port);


            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            System.out.println("Expected 8 parameters: queue name, rmq host, client1 host, client1 port, client2 host, client2 port, client3 host, client3 port");
        }


    }
}
