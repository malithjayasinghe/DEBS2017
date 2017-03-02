package org.wso2.siddhi.debs2017.input;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
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

    public void consume() {
        ConnectionFactory factory = new ConnectionFactory();

        //set connection info
        factory.setHost("localhost");
        factory.setUsername("guest");
        factory.setPassword("guest");

        //create connection
        Connection connection = null;
        Channel channel = null;
        try {
            connection = factory.newConnection();
            channel = connection.createChannel();



            QueueingConsumer consumer = new QueueingConsumer(channel);

            channel.basicConsume("test",true, consumer);


        boolean removeAll = true;

        while (true) {
            QueueingConsumer.Delivery delivery = null;

                delivery = consumer.nextDelivery();

            if (delivery==null){
                break;
            }
            if(processmessage(delivery)){
                long deliveryTag = delivery.getEnvelope().getDeliveryTag();

                    channel.basicAck(deliveryTag, removeAll);

            }
        }

        channel.close();
        connection.close();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    private static boolean processmessage(QueueingConsumer.Delivery delivery)  {

        String msg = null;
        try {
            msg = new String(delivery.getBody(), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        System.out.println("Recieved :"+delivery.getEnvelope().isRedeliver()+"\t message :"+msg);
        return false;
    }
}
