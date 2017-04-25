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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

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
 * DebsMetaData
 */
public class DebsMetaData {

    private static final Logger logger = LoggerFactory.getLogger(DebsMetaData.class);
    private static final String queryString =
            "SELECT ?machine  ?dimension ?clusters  ?threshold WHERE { " +
                    "?machine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://w" +
                    "ww.agtinternational.com/ontologies/WeidmullerMetadata#MoldingMachine> ." +
                    "?machine <http://www.agtinternational.com/ontologies/IoTCore#hasModel>" +
                    " ?model ." +
                    "?model <http://purl.oclc.org/NET/ssnx/ssn#hasProperty> ?dimension ." +
                    "?dimension <http://www.agtinternational.com/ontologies/WeidmullerMetadata#has" +
                    "NumberOfClusters> ?clusters ." +
                    "?a <http://www.agtinternational.com/ontologies/WeidmullerMetadata#isThreshold" +
                    "ForProperty> ?dimension ." +
                    "?a <http://www.agtinternational.com/ontologies/IoTCore#valueLiteral> ?threshold ." +
                    "}";
    private static HashMap<String, MetaDataItem> meta = new HashMap<>();

    /**
     * Adds a value to meta data container
     *
     * @param dimension            dimension
     * @param clusterCenters       cluster center
     * @param probabilityThreshold probability threshold
     */
    private static void addValue(String dimension, int clusterCenters,
                                 double probabilityThreshold) {
        MetaDataItem dm = new MetaDataItem(dimension, clusterCenters, probabilityThreshold);
        String mapKey = dm.getDimension();
        meta.put(mapKey, dm);
    }

    /**
     * @return the meta data holder (i.e. hash map)
     */
    public static HashMap<String, MetaDataItem> getMetaData() {
        return meta;
    }

    /**
     * Populate the meta data container (i.e. the hash map)
     *
     * @param datafile the name of the file
     */
    public static void generate(String datafile, int machines) {

        Model model = RDFDataMgr.loadModel(datafile);
        Query query = QueryFactory.create(queryString);
        try {
            for (int i = 0; i < machines; i++) {
                QueryExecution qexec = QueryExecutionFactory.create(query, model);
                ResultSet results = qexec.execSelect();
                results = ResultSetFactory.copyResults(results);
                for (; results.hasNext(); ) {
                    QuerySolution solution = results.nextSolution();
                    Resource property = solution.getResource("dimension");
                    Literal cluster = solution.getLiteral("clusters");
                    Literal thresh = solution.getLiteral("threshold");
                    String dimension = property.getLocalName().replace("_59_", "_" + i + "_");
                    DebsMetaData.addValue(dimension
                            , cluster.getInt(), thresh.getDouble());
                }
            }
        } catch (Exception e) {
            logger.debug(e.getMessage());
        }
    }

    /**
     * Loading the actual meta data file from the platform node
     *
     * @param datafile the name of the file to be loaded
     */
    public static void load(String datafile) {
        String data = "";
        Model model = RDFDataMgr.loadModel(datafile);
        Query query = QueryFactory.create(queryString);
        try {
            QueryExecution qexec = QueryExecutionFactory.create(query, model);
            ResultSet results = qexec.execSelect();
            results = ResultSetFactory.copyResults(results);
            for (; results.hasNext(); ) {
                QuerySolution solution = results.nextSolution();
                Resource property = solution.getResource("dimension");
                // Resource machine = solution.getResource("machine");
                Literal cluster = solution.getLiteral("clusters");
                Literal thresh = solution.getLiteral("threshold");
                // DebsMetaData db = new DebsMetaData();
                DebsMetaData.addValue(property.getLocalName()
                        , cluster.getInt(), thresh.getDouble());
            }
        } catch (Exception e) {
            logger.debug(e.getMessage());
        }

    }
}
