package org.wso2.siddhi.debs2017.transport;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.log4j.Logger;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.debs2017.input.SiddhiDataPublisher;
import org.wso2.siddhi.tcp.transport.TcpNettyClient;


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
public class SiddhiClient {



    public static void main(String[] args) {
        TcpNettyClient siddhiClient = new TcpNettyClient();
        TcpNettyClient siddhiClient1 = new TcpNettyClient();
        TcpNettyClient siddhiClient2 = new TcpNettyClient();
        siddhiClient.connect("localhost", 8080);
        siddhiClient1.connect("localhost", 8081);
       siddhiClient2.connect("localhost", 8082);

        SiddhiDataPublisher dp = new SiddhiDataPublisher("data_rdf10.csv");
        while(!dp.IS_END){
            Event event = dp.publishData();
            Object[] o = event.getData();
            if(o[0].toString().length()>15){
                if((Integer.parseInt(o[0].toString().substring(15)))%3==0){
                    siddhiClient.send("input", new Event[]{event});
                } else if((Integer.parseInt(o[0].toString().substring(15)))%3==1){
                   siddhiClient1.send("input", new Event[]{event});
                } else if((Integer.parseInt(o[0].toString().substring(15)))%3==2){
                   siddhiClient2.send("input", new Event[]{event});
                }
            }

        }





    }
}
