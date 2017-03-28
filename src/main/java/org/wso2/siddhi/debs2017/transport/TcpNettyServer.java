package org.wso2.siddhi.debs2017.transport;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.log4j.Logger;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.tcp.transport.callback.StreamListener;
import org.wso2.siddhi.tcp.transport.config.ServerConfig;
import org.wso2.siddhi.tcp.transport.handlers.EventDecoder;
import org.wso2.siddhi.tcp.transport.utils.StreamTypeHolder;

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
public class TcpNettyServer {

    private static final Logger log = Logger.getLogger(org.wso2.siddhi.tcp.transport.TcpNettyServer.class);
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private StreamTypeHolder streamInfoHolder = new StreamTypeHolder();
    private ChannelFuture channelFuture;
    private String hostAndPort;


    public void bootServer(ServerConfig serverConfig) {
        bossGroup = new NioEventLoopGroup(serverConfig.getReceiverThreads());
        workerGroup = new NioEventLoopGroup(serverConfig.getWorkerThreads());
        hostAndPort = serverConfig.getHost() + ":" + serverConfig.getPort();
        try {
            // More terse code to setup the server
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer() {

                        @Override
                        protected void initChannel(Channel channel) throws Exception {
                            ChannelPipeline p = channel.pipeline();
                            p.addLast(
                                    new EventDecoder(streamInfoHolder));
                            //  System.out.println(streamInfoHolder);
                        }
                    });

            // Bind and start to accept incoming connections.
            channelFuture = bootstrap.bind(serverConfig.getHost(), serverConfig.getPort()).sync();
            log.info("Tcp Server started in " + hostAndPort + "");
        } catch (InterruptedException e) {
            log.error("Error when booting up tcp server on '" + hostAndPort + "' " + e.getMessage(), e);
        }
    }

    public void shutdownGracefully() {
        channelFuture.channel().close();
        try {
            channelFuture.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            log.error("Error when shutdowning the tcp server " + e.getMessage(), e);
        }
        workerGroup.shutdownGracefully();
        bossGroup.shutdownGracefully();
        log.info("Tcp Server running on '" + hostAndPort + "' stopped.");
        //workerGroup = null;
        //bossGroup = null;

    }

    public void addStreamListener(StreamListener streamListener) {
        streamInfoHolder.putStreamCallback(streamListener);
    }

    public void removeStreamListener(String streamId) {
        streamInfoHolder.removeStreamCallback(streamId);

    }
}