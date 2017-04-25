package org.wso2.siddhi.debs2017;

import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFactory;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;
import org.apache.jena.riot.RDFDataMgr;
import org.wso2.siddhi.debs2017.input.metadata.DebsMetaData;

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
public class SampleTestCase {

    @org.junit.Test
    public void Test1() {

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
                "ORDER BY (?timestamp)" +
                "";


        //  StreamRDF destination = new StreamRDF();
        //  RDFDataMgr.parse(destination, "https://ckan.project-hobbit.eu/dataset/fd77e948-c193-4233-8842-fcc84e197491/resource/b0ead17d-65f5-448f-84ff-25d40df652a3/download/iotcore.ttl") ;

        try {
            Model model = RDFDataMgr.loadModel("molding_machine_1M.nt");

            com.hp.hpl.jena.query.Query query = QueryFactory.create(queryString);
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

                String machineName = machine.getLocalName();
                String dimension = property.getLocalName();
                if (DebsMetaData.getMetaData().keySet().contains(machineName + dimension) && !value.toString().contains("#string")) { //&& property.getLocalName().equals("_59_5")
                    if (property.getLocalName().equals("_59_31")) {
                        System.out.println(ob.getLocalName() + "\t" + machine.getLocalName() + "\t" + time.getLocalName() + "\t" + timestamp.getValue() + "\t" + property.getLocalName() + "\t" + value.getDouble());

                    }

                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        //tw

    }


}
