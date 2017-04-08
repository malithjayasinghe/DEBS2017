package org.wso2.siddhi.debs2017.input.metadata;

import com.hp.hpl.jena.query.Query;
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
public class DebsMetaDataQuery {

    public static void run(String datafile) {
        String data = "";
        Model model = RDFDataMgr.loadModel(datafile);
        String queryString =
                "SELECT ?machine  ?dimension ?clusters  ?threshold WHERE { " +
                        "?machine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.agtinternational.com/ontologies/WeidmullerMetadata#MoldingMachine> ." +
                        "?machine <http://www.agtinternational.com/ontologies/IoTCore#hasModel> ?model ." +
                        "?model <http://purl.oclc.org/NET/ssnx/ssn#hasProperty> ?dimension ." +
                        "?dimension <http://www.agtinternational.com/ontologies/WeidmullerMetadata#hasNumberOfClusters> ?clusters ." +
                        "?a <http://www.agtinternational.com/ontologies/WeidmullerMetadata#isThresholdForProperty> ?dimension ." +
                        "?a <http://www.agtinternational.com/ontologies/IoTCore#valueLiteral> ?threshold ." +
                        "}";
        Query query = QueryFactory.create(queryString);
        try {
            QueryExecution qexec = QueryExecutionFactory.create(query, model);
            ResultSet results = qexec.execSelect();
            results = ResultSetFactory.copyResults(results);
            for (; results.hasNext(); ) {
                QuerySolution solution = results.nextSolution();
                Resource property = solution.getResource("dimension");
                Resource machine = solution.getResource("machine");
                Literal cluster = solution.getLiteral("clusters");
                Literal thresh = solution.getLiteral("threshold");

                //System.out.println(machine.getLocalName()+"\t"+property.getLocalName()+"\t"+cluster.getInt()+"\t"+thresh.getDouble());

                DebsMetaData db = new DebsMetaData(machine.getLocalName(), property.getLocalName()
                        , cluster.getInt(), thresh.getDouble());
                DebsMetaData.storeValues(db);

            }
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

}
