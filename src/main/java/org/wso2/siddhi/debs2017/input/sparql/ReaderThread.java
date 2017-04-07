package org.wso2.siddhi.debs2017.input.sparql;

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
import org.apache.commons.io.IOUtils;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.debs2017.input.UnixConverter;
import org.wso2.siddhi.debs2017.input.metadata.DebsMetaData;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

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
public class ReaderThread implements Runnable {

    private String data;
    private  static int count =0;
    private LinkedBlockingQueue<Event> queue;



    public ReaderThread(String data, LinkedBlockingQueue<Event> queue) {
        this.data = data;
        this.queue = queue;

    }
    public ReaderThread(String data) {
        this.data = data;


    }


    static synchronized public int getCount() {
        return count;
    }

    @Override
    public void run() {

        this.queue = SparQLProcessor.arrayList.get(Integer.parseInt(Thread.currentThread().getName()));
        String queryString = "" +
                "SELECT ?machine ?time ?timestamp ?dimension ?value" +
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

        try {
            Model model = ModelFactory.createDefaultModel().read(IOUtils.toInputStream(this.data, "UTF-8"), null, "TURTLE");

            Query query = QueryFactory.create(queryString);
            QueryExecution qexec = QueryExecutionFactory.create(query, model);
            ResultSet results = qexec.execSelect();
            results = ResultSetFactory.copyResults(results);
            for (; results.hasNext(); ) {
                QuerySolution solution = results.nextSolution();

                Resource time = solution.getResource("time"); // Get a result variable - must be a resource
                Resource property = solution.getResource("dimension");
                Resource machine = solution.getResource("machine");
                Literal timestamp = solution.getLiteral("timestamp");
                Literal value = solution.getLiteral("value");
                if (!value.toString().contains("#string")) {


                   // int machineNo = Integer.parseInt(machine.getLocalName().substring(8));
                    String stateful = property.getLocalName();
                    if(DebsMetaData.meta.containsKey(stateful)) {

                        int centers = DebsMetaData.meta.get(stateful).getClusterCenters();
                        double probability = DebsMetaData.meta.get(stateful).getProbabilityThreshold();

                        Event event = new Event(System.currentTimeMillis(), new Object[]{
                                machine.getLocalName(),
                                time.getLocalName(),
                                property.getLocalName(),
                                UnixConverter.getUnixTime(timestamp.getLexicalForm()),
                                Math.round(value.getDouble() * 10000.0) / 10000.0, //
                                centers,
                                probability,
                                0});

                        this.queue.put(event);
                    }
                    //System.out.println(machine.getLocalName()+"\t"+time.getLocalName()+"\t"+timestamp.getValue()+"\t"+property.getLocalName()+"\t"+value.getFloat());
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }



}
