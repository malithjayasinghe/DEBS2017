package org.wso2.siddhi.debs2017.input.sparql;


import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.apache.commons.io.IOUtils;
import org.apache.jena.riot.RDFDataMgr;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.executor.math.mod.ModExpressionExecutorLong;
import org.wso2.siddhi.debs2017.input.UnixConverter;
import org.wso2.siddhi.debs2017.input.metadata.MetaDataQueryMulti;
import org.wso2.siddhi.debs2017.transport.TcpNettyClient;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;

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
public class SparQLProcessor extends DefaultConsumer {

    static int value = 10;

    static ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("%d").build();
    public  static  final ExecutorService EXECUTOR = Executors.newFixedThreadPool(value, threadFactory);
    public static ArrayList<LinkedBlockingQueue<Event>> arrayList = new ArrayList<>(value);



    public SparQLProcessor(Channel channel, String host1, int port1, String host2, int port2, String host3, int port3) {
        super(channel);
        for (int i=0; i<value; i++){
            arrayList.add(new LinkedBlockingQueue());
        }
        Collections.synchronizedList(arrayList);
        SorterThread sort = new SorterThread(arrayList, host1, port1, host2, port2, host3, port3);
        sort.start();
        MetaDataQueryMulti.run("molding_machine_10M.metadata_old.nt");
    }

    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        String msg = new String(body, "UTF-8");
        Runnable reader = new ReaderThread(msg);
        EXECUTOR.execute(reader);


    }



}