package org.wso2.siddhi.debs2017.input.sparql;


import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.debs2017.input.metadata.DebsMetaData;
import org.wso2.siddhi.debs2017.query.CentralDispatcher;
import org.wso2.siddhi.debs2017.transport.utils.TcpNettyClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.*;

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
*
* Publishers spaql jobs to executor thread pool + start the sorting threads which publishes events to the back-end workers
*/
public class EventDispatcher extends DefaultConsumer {

    private static final Logger logger = LoggerFactory.getLogger(EventDispatcher.class);
    public static TcpNettyClient siddhiClient0 = new TcpNettyClient();
    public static TcpNettyClient siddhiClient1 = new TcpNettyClient();
    /**
     * The constructor
     *
     * @param channel the channel
     * @param host1   the host 1
     * @param port1   the port 1
     * @param host2   the host 2
     * @param port2   the port 2
     */
    public EventDispatcher(Channel channel, String host1, int port1, String host2, int port2) {
        super(channel);
       this.siddhiClient0.connect(host1, port1);
       this.siddhiClient1.connect(host2, port2);
    }

    /**
     * Handle delivery
     *
     * @param consumerTag the consumer tag
     * @param envelope    the envelop
     * @param properties  the properties
     * @param body        the body
     * @throws IOException thrown if an IO exception occurs
     */
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {

        if (body.length<30) {
            System.out.println("event - Terminated");
            try {
                getChannel().close();
                getChannel().getConnection().close();
                RegexPatternSearch.publishTerminate();

            } catch (IOException e) {
                logger.debug(e.getMessage());
            } catch (TimeoutException e) {
                logger.debug(e.getMessage());
            }

        } else {

            long time = System.nanoTime();
            RegexPatternSearch regexPatternSearch = new RegexPatternSearch(body,time);
            regexPatternSearch.process();

        }
    }

}