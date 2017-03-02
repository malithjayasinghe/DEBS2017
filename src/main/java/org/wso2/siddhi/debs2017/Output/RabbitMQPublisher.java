package org.wso2.siddhi.debs2017.Output;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
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
public class RabbitMQPublisher {

    public void publish()  {
        ConnectionFactory factory = new ConnectionFactory();

        //set connection info
        factory.setHost("localhost");
        factory.setUsername("guest");
        factory.setPassword("guest");

        //create connection
        Connection connection = null;
        try {
            connection = factory.newConnection();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }

        //create a channel
        String qName = "test";
        Channel channel = null;
        try {
            channel = connection.createChannel();
            //this line is required to initialize a queue
            channel.queueDeclare(qName, false, false, false, null);
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (int i =0; true; i++) {
            String message = "Hello World --"+i;
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // e.printStackTrace();
            }
            try {
                channel.basicPublish("", qName, null, message.getBytes());
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println("Sent :"+message);
        }

        // channel.close();
        //connection.close();


    }
}
