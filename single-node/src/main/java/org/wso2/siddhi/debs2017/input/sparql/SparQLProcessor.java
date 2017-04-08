package org.wso2.siddhi.debs2017.input.sparql;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFactory;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;
import com.lmax.disruptor.RingBuffer;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.apache.commons.io.IOUtils;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.debs2017.input.metadata.DebsMetaDataQuery;
import org.wso2.siddhi.debs2017.input.metadata.SampleDebsMetaData;
import org.wso2.siddhi.debs2017.processor.EventWrapper;

import java.io.IOException;
import java.util.ArrayList;
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

    private static int value = 10;


    private static ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("%d").build();
    private static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(value, threadFactory);
    public static ArrayList<LinkedBlockingQueue<Event>> arrayList = new ArrayList<>(value);

    private final static String queryString = "" +
            "SELECT ?observation ?machine ?time ?timestamp ?dimension ?value" +
            " WHERE {" +
            "?observation <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.agtinternational.com/ontologies/I4.0#MoldingMachineObservationGroup> ." +
            "?observation <http://www.agtinternational.com/ontologies/I4.0#machine> ?machine ." +
            "?observation <http://purl.oclc.org/NET/ssnx/ssn#observationResultTime> ?time ." +
            "?time <http://www.agtinternational.com/ontologies/IoTCore#valueLiteral> ?timestamp ." +
            "?observation <http://www.agtinternational.com/ontologies/I4.0#contains> ?obGroup ." +
            "?obGroup <http://purl.oclc.org/NET/ssnx/ssn#observedProperty> ?dimension ." +
            "?obGroup <http://purl.oclc.org/NET/ssnx/ssn#observationResult> ?output ." +
            "?output <http://purl.oclc.org/NET/ssnx/ssn#hasValue> ?valID ." +
            "?valID <http://www.agtinternational.com/ontologies/IoTCore#valueLiteral> ?value . " +
            "}" +
            "" +
            "";

    public SparQLProcessor(Channel channel, RingBuffer<EventWrapper> ringBuffer) {
        super(channel);
        for (int i = 0; i < value; i++) {
            arrayList.add(new LinkedBlockingQueue());
        }
        Collections.synchronizedList(arrayList);
        SorterThread sort = new SorterThread(arrayList, ringBuffer);
        sort.start();
        SampleDebsMetaData.run("molding_machine_10M.metadata.nt");
    }

    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        String msg = new String(body, "UTF-8");
        Runnable reader = new ReaderRunnable(msg);
        EXECUTOR.execute(reader);


    }


}
