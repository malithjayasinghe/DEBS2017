package org.wso2.siddhi.debs2017.input.sparql;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.RingBuffer;
import com.rabbitmq.client.*;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.debs2017.input.UnixConverter;
import org.wso2.siddhi.debs2017.input.metadata.DebsMetaData;
import org.wso2.siddhi.debs2017.processor.EventWrapper;
import org.wso2.siddhi.debs2017.query.SingleNodeServer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
public class CentralDispatcher extends DefaultConsumer {

    public static int count = 0;
    public static long bytesRec = 0l;

    /**
     * Dispatchers events to the disruptor after sorting
     *  @param channel the channel
     *
     */
    public CentralDispatcher(Channel channel) {

        super(channel);

    }

    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {


        if(body.length<30){

            try {
                getChannel().close();
                getChannel().getConnection().close();
                RegexPattern.publishTerminate(System.nanoTime());
            } catch (Exception e) {
                e.printStackTrace();
            }

        } else {
                count++;
                bytesRec += body.length;
               RegexPattern regexPattern = new RegexPattern(body,System.nanoTime());
               regexPattern.process();


        }



    }

}
