package org.wso2.siddhi.debs2017.output;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;
import org.hobbit.core.data.RabbitQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.debs2017.input.sparql.CentralDispatcher;
import org.wso2.siddhi.debs2017.query.SingleNodeServer;

import java.io.IOException;
import java.io.StringWriter;


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

/**
 * Alert Generator
 */
public class AlertGenerator {

    private static int anomalyCount = 0;
    private Double probThresh;
    private String timestamp;
    private String dimension;
    private String machineNumber;
    private String anomaly = "http://project-hobbit.eu/resources/debs2017#";
    private String ar = "http://www.agtinternational.com/ontologies/DEBSAnalyticResults#";
    private String rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
    private String debs = "http://project-hobbit.eu/resources/debs2017#";
    private String xsd = "http://www.w3.org/2001/XMLSchema#";
    private String wmm = "http://www.agtinternational.com/ontologies/WeidmullerMetadata#";
    private String i40 = "http://www.agtinternational.com/ontologies/I4.0#";
    private RabbitQueue rabbitMQPublisher;
    private StringWriter out;
    private long dispatchedTime;
    private double sum = 0;

    private Channel channel;
    private static final Logger logger = LoggerFactory.getLogger(AlertGenerator.class);



    /**
     * initialize the parameters from the sidhhi event, to generate the alert
     *
     * @param rabbitMQPublisher publish to rabbitmq
     */
    public AlertGenerator(RabbitQueue rabbitMQPublisher) {
        this.rabbitMQPublisher = rabbitMQPublisher;
        this.channel = rabbitMQPublisher.getChannel();

    }

    /**
     * generate the rdf model and publish to rabbitmq
     */
    public void generateAlert(Event event) {

        publishEvent(event);
    }


    public void publishEvent(Event event) {
        this.probThresh = Double.parseDouble(event.getData()[3].toString());
        this.timestamp = (String) event.getData()[1];
        this.dimension = (String) event.getData()[2];
        this.machineNumber = (String) event.getData()[0];
        this.dispatchedTime = event.getTimestamp();
        Model model = ModelFactory.createDefaultModel();
        String anomalyName = "Anomaly_" + anomalyCount;
        Resource r1 = model.createResource(anomaly + anomalyName);
        Property type = model.createProperty(rdf + "type");
        Resource r2 = model.createResource(ar + "Anomaly");
        Property threshProb = model.createProperty(ar + "hasProbabilityOfObservedAbnormalSequence");
        Property time = model.createProperty(ar + "hasTimeStamp");
        Resource r4 = model.createResource(debs + timestamp);
        Property dim = model.createProperty(ar + "inAbnormalDimension");
        Resource r5 = model.createResource(wmm + dimension);
        Property machine = model.createProperty(i40 + "machine");
        Resource r6 = model.createResource(wmm + machineNumber);

        model.add(r1, threshProb, model.createTypedLiteral(probThresh))
                .add(r1, time, r4)
                .add(r1, dim, r5)
                .add(r1, machine, r6)
                .add(r1, type, r2);

        anomalyCount++;
        String str = "N-TRIPLES";
        out = new StringWriter();
        model.write(out, str);

        try {
            this.channel.basicPublish("", rabbitMQPublisher.getName(),
                    MessageProperties.PERSISTENT_BASIC, out.toString().getBytes());
        } catch (IOException e) {
            logger.debug(e.getMessage());
        }
        System.out.println(out.toString());
        sum += System.nanoTime() - dispatchedTime;

    }

    public void terminate() {

        long endTime = System.currentTimeMillis();
        double runTime = (endTime - SingleNodeServer.startime) / 1000.0;
        logger.info("Running time in sec\t:" + runTime);
        logger.info("Average throghput(msg)\t:" + CentralDispatcher.count / runTime);
        logger.info("Average throghput(bytes)\t:" + CentralDispatcher.bytesRec / runTime);
        logger.info("Average Latency : " + ((sum / 1000000) / anomalyCount));

        String terminationMessage = "~~Termination Message~~";
        try {
            this.channel.basicPublish("", rabbitMQPublisher.getName(),
                    MessageProperties.PERSISTENT_BASIC, terminationMessage.getBytes());
        } catch (IOException e) {
            logger.debug(e.getMessage());
        }

        //System.out.println("Threads end : "+Thread.activeCount());
    }
}
