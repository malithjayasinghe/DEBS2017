package org.wso2.siddhi.debs2017.transport;


import org.wso2.siddhi.debs2017.output.RabbitMQPublisher;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.debs2017.transport.utils.TcpNettyServer;
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
    public static void start(String[] args) {
        if (args.length == 3) {
            String host = args[0];
            int port = Integer.parseInt(args[1]);
            StreamDefinition streamDefinition = StreamDefinition.id("output").
                    attribute("machine", Attribute.Type.STRING).
                    attribute("time", Attribute.Type.STRING).
                    attribute("dimension", Attribute.Type.STRING).
                    attribute("value", Attribute.Type.DOUBLE).
                    attribute("threshold", Attribute.Type.DOUBLE).
                    attribute("node", Attribute.Type.INT);
            RabbitMQPublisher rmq = new RabbitMQPublisher("output");
            TcpNettyServer tcpNettyServer = new TcpNettyServer();
            tcpNettyServer.addStreamListener(new OutputListener(streamDefinition, rmq));
            ServerConfig serverConfig = new ServerConfig();
            serverConfig.setHost(host);
            serverConfig.setPort(port);
            tcpNettyServer.bootServer(serverConfig);
        } else {
            System.out.println("Expected two parameters : host , port");
        }

    }

}
