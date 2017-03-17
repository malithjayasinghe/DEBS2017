package org.wso2.siddhi.debs2017.transport;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.debs2017.transport.processor.DebsAnormalyDetector;
import org.wso2.siddhi.debs2017.transport.processor.EventWrapper;
import org.wso2.siddhi.debs2017.transport.processor.SiddhiEventHandler;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.tcp.transport.TcpNettyServer;
import org.wso2.siddhi.tcp.transport.config.ServerConfig;

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
public class SiddhiServer {

    public static void main(String[] args) {

        StreamDefinition streamDefinition = StreamDefinition.id("input").
                attribute("machine", Attribute.Type.STRING).
                attribute("time", Attribute.Type.STRING).
                attribute("sentTime", Attribute.Type.STRING).
                attribute("uTime", Attribute.Type.LONG).
                attribute("dimension",Attribute.Type.STRING).
                attribute("value", Attribute.Type.DOUBLE).
                attribute("ij_timestamp", Attribute.Type.LONG);
        TcpNettyServer tcpNettyServer = new TcpNettyServer();


        Executor executor = Executors.newCachedThreadPool();
        int buffersize = 128;
        Disruptor<EventWrapper> disruptor = new Disruptor<>(EventWrapper::new,buffersize, executor);

        RingBuffer<EventWrapper> ring = disruptor.getRingBuffer();

        SiddhiEventHandler sh1 = new SiddhiEventHandler(0L, 3L, ring);
        SiddhiEventHandler sh2 = new SiddhiEventHandler(1L, 3L, ring);
        SiddhiEventHandler sh3 = new SiddhiEventHandler(2L, 3L, ring);

        DebsAnormalyDetector debsAnormalyDetector = new DebsAnormalyDetector();

       disruptor.handleEventsWith(sh1,sh2, sh3);
       disruptor.after(sh1, sh2, sh3).handleEventsWith(debsAnormalyDetector);
       // disruptor.handleEventsWith(debsAnormalyDetector);

        disruptor.start();
        tcpNettyServer.addStreamListener(new Listener(streamDefinition, ring));

        ServerConfig serverConfig = new ServerConfig();
        serverConfig.setHost("localhost");
        serverConfig.setPort(8082);
        tcpNettyServer.bootServer(serverConfig);

        //tcpNettyServer.shutdownGracefully();
    }

}
