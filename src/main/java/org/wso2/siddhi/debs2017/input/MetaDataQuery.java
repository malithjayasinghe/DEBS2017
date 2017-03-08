package org.wso2.siddhi.debs2017.input;

import com.hp.hpl.jena.query.*;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;
import org.apache.jena.base.Sys;
import org.apache.jena.riot.RDFDataMgr;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
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
public class MetaDataQuery {
    private static ArrayList<String> str = new ArrayList<String>();

    public static void run() {
        String data = "";
        Model model = RDFDataMgr.loadModel("sample_metadata_1machine.nt");
        String queryString =
                "SELECT ?machine ?model ?dimension ?clusters ?isStateful ?threshold WHERE { " +
                        "?machine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.agtinternational.com/ontologies/WeidmullerMetadata#MoldingMachine> ." +
                        "?machine <http://www.agtinternational.com/ontologies/IoTCore#hasModel> ?model ." +
                        "?model <http://purl.oclc.org/NET/ssnx/ssn#hasProperty> ?dimension ." +
                        "?dimension <http://www.agtinternational.com/ontologies/WeidmullerMetadata#hasNumberOfClusters> ?clusters ." +
                        "?dimension <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?isStateful ." +
                        "?a <http://www.agtinternational.com/ontologies/WeidmullerMetadata#isThresholdForProperty>?dimension ." +
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
                Resource model_ = solution.getResource("model");
                Literal cluster = solution.getLiteral("clusters");
                Resource state = solution.getResource("isStateful");
                Literal thresh = solution.getLiteral("threshold");
                str.add(data);
                DebsMetaData db = new DebsMetaData(machine.getLocalName(), model_.getLocalName(), property.getLocalName()
                        , cluster.getInt(), thresh.getDouble(), 50);
                DebsMetaData.storeValues(db);

            }
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
