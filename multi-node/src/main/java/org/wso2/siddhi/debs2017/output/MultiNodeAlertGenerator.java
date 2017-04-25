/*
 *
 *  Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * /
 *
 */

package org.wso2.siddhi.debs2017.output;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;
import org.hobbit.core.data.RabbitQueue;
import org.wso2.siddhi.core.event.Event;

import java.io.StringWriter;


/**
 * Created by sachini on 3/23/17.
 */
public class MultiNodeAlertGenerator {
    private static int anomalyCount = 0;
    private String probThresh;
    private String timestamp;
    private String dimension;
    private String machineNumber;
    String anomaly = "http://wso2.org.debsgrandchallenge.anomaly#";
    String ar = "http://www.agtinternational.com/ontologies/DEBSAnalyticResults#";
    String rdf = "http://www.agtinternational.com/ontologies/DEBSAnalyticResults#";
    String debs = "http://project-hobbit.eu/resources/debs2017#";
    String xsd = "http://www.w3.org/2001/XMLSchema#";
    String wmm = "http://www.agtinternational.com/ontologies/WeidmullerMetadata#";
    String i40 = "http://www.agtinternational.com/ontologies/I4.0#";
    private RabbitQueue rabbitQueue;
    private StringWriter out;
    private long dispatchedTime;
    private static double sum = 0;

    /**
     * initialize the parameters from the sidhhi event, to generate the alert
     *
     * @param rabbitQueue publish to rabbitmq
     */
    public MultiNodeAlertGenerator(RabbitQueue rabbitQueue) {

        this.rabbitQueue = rabbitQueue;
    }

    /**
     * generate the rdf model and publish to rabbitmq
     */
    public void generateAlert(Event event) {
        this.probThresh = Double.toString((Double) event.getData()[3]);
        this.timestamp = transformTimestamp((String) event.getData()[1]);
        this.dimension = (String) event.getData()[2];
        this.machineNumber = (String) event.getData()[0];
        this.dispatchedTime = event.getTimestamp();


        Model model = ModelFactory.createDefaultModel();
        String anomalyName = "Anomaly_" + anomalyCount;
        Resource r1 = model.createResource(anomaly + anomalyName);
        Property type = model.createProperty(rdf + "type");
        Resource r2 = model.createResource(ar + "Anomaly");
        Property threshProb = model.createProperty(ar + "hasProbabilityOfObservedAbnormalSequence");
        Property time = model.createProperty(ar + "hasTimestamp");
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
        //rabbitMQPublisher.publish(out.toString());
        Channel channel = rabbitQueue.getChannel();
        try {
            channel.basicPublish("", rabbitQueue.getName(), MessageProperties.PERSISTENT_BASIC, out.toString().getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }
        sum += System.currentTimeMillis() - dispatchedTime;
        if (anomalyCount == 2000) {
            System.out.println("Average Latency : " + (sum / anomalyCount));
        }


    }

    /**
     * Transforms the timestamp
     *
     * @param time time stamp to be converted
     * @return converted value
     */
    private String transformTimestamp(String time) {
        String[] str = time.split("_");
        return str[0].concat("_" + (Integer.parseInt(str[1]) - 5));
    }


    public void terminate() {

       /* long endTime = System.currentTimeMillis();
       // double runTime = (endTime - SingleNodeServer.startime) / 1000.0;
        System.out.println("Running time in sec\t:" + runTime);
        System.out.println("Average throghput(msg)\t:" + CentralDispatcher.count / runTime);
        System.out.println("Average throghput(bytes)\t:" + CentralDispatcher.bytesRec / runTime);
        System.out.println("Average Latency : " + (sum / anomalyCount));*/
        Channel channel = rabbitQueue.getChannel();
        String TERMINATION_MESSAGE = "~~Termination Message~~";
        try {
            channel.basicPublish("", rabbitQueue.getName(), MessageProperties.PERSISTENT_BASIC, TERMINATION_MESSAGE.toString().getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
