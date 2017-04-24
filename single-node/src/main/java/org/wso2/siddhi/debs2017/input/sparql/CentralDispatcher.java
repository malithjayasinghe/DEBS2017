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

    private static ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("%d").build();
    private static ExecutorService EXECUTOR;
    private  ArrayList<LinkedBlockingQueue<Event>> arrayList = SingleNodeServer.arraylist;
    private static final String TERMINATION_MESSAGE = "~~Termination Message~~";
    private AtomicBoolean isSparQL = SingleNodeServer.isSparQL;
    public static int count = 0;
    public static long bytesRec = 0l;
    public RingBuffer<RabbitMessage> ringBuffer;

    private static Pattern patternTime = Pattern.compile("<http://purl.oclc.org/NET/ssnx/ssn#observationResultTime>.<http://project-hobbit.eu/resources/debs2017#(.*)>");
    private static Pattern patternTimestamp = Pattern.compile("<http://www.agtinternational.com/ontologies/IoTCore#valueLiteral>.\"(.*)\"\\^\\^<http://www.w3.org/2001/XMLSchema#dateTime>");
    private static Pattern patternMachine = Pattern.compile("<http://www.agtinternational.com/ontologies/I4.0#machine>.<http://www.agtinternational.com/ontologies/WeidmullerMetadata#(.*)>");
    Channel channel = null;
    Connection con = null;
    /**
     * Dispatchers events to the disruptor after sorting
     *  @param channel the channel
     *
     * @param connection
     * @param executorSize the size of the executor pool
     */
    public CentralDispatcher(Channel channel, Connection connection, int executorSize) {

        super(channel);
        EXECUTOR = Executors.newFixedThreadPool(executorSize, threadFactory);
        this.channel = channel;
        this.con = connection;



    }

    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
     //   System.out.println(Thread.currentThread()+"\t"+body.length);

        if(body.length<30){

            try {
                getChannel().close();
                getChannel().getConnection().close();
               // for(int i =0; i<arrayList.size(); i++){
                   // ObservationGroup ob = new ObservationGroup(-1l, null);
                    RegexPattern.publishTerminate(System.nanoTime());
               // }
                System.out.println("termination received");
//                channel.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
//            con.close();

        } else {

                count++;
                bytesRec += body.length;

               RegexPattern regexPattern = new RegexPattern(body,System.nanoTime());
               regexPattern.process();


        }



    }

    private String processMachine(String msg) {
        String machine = "";

        Matcher matcher3 = patternMachine.matcher(msg);
        while (matcher3.find()) {
            machine = matcher3.group(1);
            break;
        }
        return machine;
    }

    private long processUTime(String msg) {

        String timeStamp = "";

        Matcher matcher2 = patternTimestamp.matcher(msg);
        while (matcher2.find()) {
            timeStamp = matcher2.group(1);
            break;
        }
        return UnixConverter.getUnixTime(timeStamp);

    }

    private String processTime(String msg) {
        String time = "";

        Matcher matcher1 = patternTime.matcher(msg);
        while (matcher1.find()) {
            time = matcher1.group(1);
            break;
        }
        return time;

    }
}
