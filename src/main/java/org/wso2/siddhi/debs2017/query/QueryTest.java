package org.wso2.siddhi.debs2017.query;

import com.hp.hpl.jena.query.*;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;
import org.apache.jena.riot.RDFDataMgr;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.debs2017.input.DataPublisher;
import org.wso2.siddhi.debs2017.input.UnixConverter;
import org.wso2.siddhi.debs2017.input.metadata.DebsMetaData;
import org.wso2.siddhi.debs2017.input.metadata.MetaDataQuery;

import java.util.ArrayList;

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
public class QueryTest {

    ArrayList<Object[]> arr = new ArrayList<>();
    public static void main(String[] args) {
        QueryTest q = new QueryTest();
        q.run();
    }



    public void run() {


        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "@config(async = 'true') \n" +
                "define stream inStream (machine string, tstamp string, uTime long, dimension string, value double, center int);";

        String query = ("" +
                "\n" +
                "from inStream\n" +
                "select machine, tstamp, dimension, uTime, str:concat(machine, '-', dimension) as partitionId, value, center \n" +
                "insert into inStreamA;\n" +
                "@info(name = 'query1') partition with ( partitionId of inStreamA) \n" +
                "begin " +
                "from inStreamA#window.externalTime(uTime , 10) \n" +
                "select machine, tstamp, uTime, dimension, debs2017:cluster(value, center) as center " +
                " insert into #outputStream; " + //inner stre
                "\n" +
                "from #outputStream \n" +
                "select machine, tstamp, dimension, debs2017:markovnew(center, 10) as probability " +
                "insert into detectAnomaly " +
                "end");
        System.out.println(inStreamDefinition + query);
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("detectAnomaly", new StreamCallback() {
            @Override
            public void receive(org.wso2.siddhi.core.event.Event[] events) {

                for(Event ev : events){

                        double val = (double)ev.getData()[3];
                       if(val<0.005) {
                            System.out.println(ev.getData()[0] + "\t" + ev.getData()[1] + "\t" + ev.getData()[2] + "\t" + ev.getData()[3] + "\n");
                            //0.004115226337448559
                        }


                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inStream");

        //extract metadata
        MetaDataQuery.run("molding_machine_10M.metadata.nt");

        //run sparql
        System.out.println(System.currentTimeMillis());
        sparql();
        try {

            for(int i =0; i<arr.size(); i++){
                inputHandler.send(arr.get(i));
              //  String abc = arr.get(i)[0]+"\t"+arr.get(i)[1]+"\t"+arr.get(i)[2]+"\t"+arr.get(i)[4];
                //System.out.println(abc);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        executionPlanRuntime.start();



    }

    private void sparql() {
            String queryString = "" +
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
               "ORDER BY (?timestamp)"+
                "";



        try {
            Model model = RDFDataMgr.loadModel("molding_machine_10M.nt");

            com.hp.hpl.jena.query.Query query = QueryFactory.create(queryString);
            QueryExecution qexec = QueryExecutionFactory.create(query, model);
            ResultSet results = qexec.execSelect();
            results = ResultSetFactory.copyResults(results);
            System.out.println(System.currentTimeMillis());
            for (; results.hasNext(); ) {
                QuerySolution solution = results.nextSolution();
                Resource ob = solution.getResource("observation");

                Resource time = solution.getResource("time"); // Get a result variable - must be a resource
                Resource property = solution.getResource("dimension");
                Resource machine = solution.getResource("machine");
                Literal timestamp = solution.getLiteral("timestamp");
                Literal value = solution.getLiteral("value");

                String machineName = machine.getLocalName();
                String dimension = property.getLocalName();
                if (DebsMetaData.meta.keySet().contains(machineName+dimension) &&  !value.toString().contains("#string")) { //&& property.getLocalName().equals("_59_5")
                       // if(property.getLocalName().equals("_59_66")) {
                            // System.out.println(ob.getLocalName()+"\t"+machine.getLocalName()+"\t"+time.getLocalName()+"\t"+timestamp.getValue()+"\t"+property.getLocalName()+"\t"+value.getDouble());
                            arr.add(new Object[]{machineName, time.getLocalName(), UnixConverter.getUnixTime(timestamp.getString()), dimension, value.getDouble(),
                                    DebsMetaData.meta.get(machineName + dimension).getClusterCenters()});
//                            long abc = arr.size();
//                            //System.out.println(machine.getLocalName()+"\t"+time.getLocalName()+"\t"+abc+"\t"+property.getLocalName()+"\t"+value.getDouble());
//                            arr.add(new Object[]{machine.getLocalName(), time.getLocalName(), abc, property.getLocalName(), value.getDouble(),
//                                    DebsMetaData.meta.get(machineName+dimension).getClusterCenters()});
                       // }


                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        //tw

    }


}
