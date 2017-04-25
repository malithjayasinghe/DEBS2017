package org.wso2.siddhi.debs2017.transport;


import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import org.hobbit.core.data.RabbitQueue;
import org.wso2.siddhi.debs2017.transport.utils.TcpNettyServer;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.tcp.transport.config.ServerConfig;

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
public class OutputServer {
    public static boolean isSort;

    public static void main(String[] args) {
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        boolean sort = Boolean.parseBoolean(args[3]);
        isSort = sort;
        if (args.length == 2) {


            DebsBenchmarkOutput dbOut = new DebsBenchmarkOutput();
            dbOut.init();

            RabbitQueue output = dbOut.getOutputQueue();
            StreamDefinition streamDefinition = StreamDefinition.id("output").
                    attribute("machine", Attribute.Type.STRING).
                    attribute("time", Attribute.Type.STRING).
                    attribute("dimension", Attribute.Type.STRING).
                    attribute("value", Attribute.Type.DOUBLE).
                    attribute("threshold", Attribute.Type.DOUBLE).
                    attribute("node", Attribute.Type.INT);
            // RabbitMQPublisher rmq = new RabbitMQPublisher(output);
            TcpNettyServer tcpNettyServer = new TcpNettyServer();
            tcpNettyServer.addStreamListener(new OutputListener(streamDefinition, output));
            ServerConfig serverConfig = new ServerConfig();
            serverConfig.setHost(host);
            serverConfig.setPort(port);
            tcpNettyServer.bootServer(serverConfig);
        } else if (args.length > 2) {

            String outputQueue = args[2];
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

            output = new RabbitQueue(channel, outputQueue);

            StreamDefinition streamDefinition = StreamDefinition.id("output").
                    attribute("machine", Attribute.Type.STRING).
                    attribute("time", Attribute.Type.STRING).
                    attribute("dimension", Attribute.Type.STRING).
                    attribute("value", Attribute.Type.DOUBLE).
                    attribute("threshold", Attribute.Type.DOUBLE).
                    attribute("node", Attribute.Type.INT);
            // RabbitMQPublisher rmq = new RabbitMQPublisher(outputQueue);
            TcpNettyServer tcpNettyServer = new TcpNettyServer();
            tcpNettyServer.addStreamListener(new OutputListener(streamDefinition, output));
            ServerConfig serverConfig = new ServerConfig();
            serverConfig.setHost(host);
            serverConfig.setPort(port);
            tcpNettyServer.bootServer(serverConfig);
            //System.out.println("Expected two parameters : host , port");
        }

    }

}
