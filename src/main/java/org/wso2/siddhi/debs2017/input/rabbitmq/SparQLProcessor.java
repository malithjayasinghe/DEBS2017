package org.wso2.siddhi.debs2017.input.rabbitmq;

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
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.apache.commons.io.IOUtils;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.debs2017.input.UnixConverter;
import org.wso2.siddhi.debs2017.input.metadata.DebsMetaData;
import org.wso2.siddhi.debs2017.input.metadata.MultiNodeMetaDataQuery;
import org.wso2.siddhi.debs2017.transport.utils.TcpNettyClient;

import java.io.IOException;

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
    private TcpNettyClient siddhiClient = new TcpNettyClient();
    private TcpNettyClient siddhiClient1 = new TcpNettyClient();
    private TcpNettyClient siddhiClient2 = new TcpNettyClient();
  //  static  int count =0;

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

    public SparQLProcessor(Channel channel, String host1, int port1, String host2, int port2, String host3, int port3) {
        super(channel);
        siddhiClient.connect(host1, port1);
        siddhiClient1.connect(host2, port2);
        siddhiClient2.connect(host3, port3);
        MultiNodeMetaDataQuery.run("molding_machine_old_10M.metadata.nt");
    }

    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        String msg = new String(body, "UTF-8");
       // System.out.println(msg);
        excuteQuery(msg);


    }

    /**
     * @param message the message which contains RDF triples
     */
    public void excuteQuery(String message) {
        //count++;
        //System.out.println(count);

        try {
            Model model = ModelFactory.createDefaultModel().read(IOUtils.toInputStream(message, "UTF-8"), null, "TURTLE");

            Query query = QueryFactory.create(queryString);
            QueryExecution qexec = QueryExecutionFactory.create(query, model);
            ResultSet results = qexec.execSelect();
            results = ResultSetFactory.copyResults(results);
            for (; results.hasNext(); ) {
                QuerySolution solution = results.nextSolution();
                Resource ob = solution.getResource("observation");

                Resource time = solution.getResource("time"); // Get a result variable - must be a resource
                Resource property = solution.getResource("dimension");
                Resource machine = solution.getResource("machine");
                Literal timestamp = solution.getLiteral("timestamp");
                Literal value = solution.getLiteral("value");
                if (!value.toString().contains("#string")) {
                    int machineNo = Integer.parseInt(machine.getLocalName().substring(15));
                    String stateful = machine.getLocalName()+"_"+machineNo+property.getLocalName();

                      if(DebsMetaData.meta.containsKey(stateful)) {

                          int centers = DebsMetaData.meta.get(stateful).getClusterCenters();
                          double probability = DebsMetaData.meta.get(stateful).getProbabilityThreshold();

//                    Event event = new Event(System.currentTimeMillis(), new Object[]{machine.getLocalName(),
//                            time.getLocalName(),timestamp.getLexicalForm(), UnixConverter.getUnixTime(timestamp.getLexicalForm()), property.getLocalName(), value.getDouble()});
                          if (machineNo % 3 == 0) {
                              Event event = new Event(System.currentTimeMillis(), new Object[]{
                                      machine.getLocalName(),
                                      time.getLocalName(),
                                      property.getLocalName(),
                                      UnixConverter.getUnixTime(timestamp.getLexicalForm()),
                                      Math.round(value.getDouble() * 10000.0) / 10000.0, //
                                      centers,
                                      probability,
                                      0});
                              siddhiClient.send("input", new Event[]{event});
                          } else if (machineNo % 3 == 1) {
                              Event event = new Event(System.currentTimeMillis(), new Object[]{
                                      machine.getLocalName(),
                                      time.getLocalName(),
                                      property.getLocalName(),
                                      UnixConverter.getUnixTime(timestamp.getLexicalForm()),
                                      Math.round(value.getDouble() * 10000.0) / 10000.0, //
                                      centers,
                                      probability,
                                      1});
                              siddhiClient1.send("input", new Event[]{event});
                          } else if (machineNo % 3 % 3 == 2) {
                              Event event = new Event(System.currentTimeMillis(), new Object[]{
                                      machine.getLocalName(),
                                      time.getLocalName(),
                                      property.getLocalName(),
                                      UnixConverter.getUnixTime(timestamp.getLexicalForm()),
                                      Math.round(value.getDouble() * 10000.0) / 10000.0, //
                                      centers,
                                      probability,
                                      2});
                              siddhiClient2.send("input", new Event[]{event});
                          }
                      }

                    // System.out.println(ob.getLocalName()+"\t"+machine.getLocalName()+"\t"+time.getLocalName()+"\t"+timestamp.getValue()+"\t"+property.getLocalName()+"\t"+value.getFloat());
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}