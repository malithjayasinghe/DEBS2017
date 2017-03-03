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


    public static void main(String[] args) {
        String data ="";
        Model model = RDFDataMgr.loadModel("sample_metadata_1machine.nt") ;
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
        Query query = QueryFactory.create(queryString) ;
        try {
            QueryExecution qexec = QueryExecutionFactory.create(query, model);
            ResultSet results = qexec.execSelect() ;
            results = ResultSetFactory.copyResults(results) ;

            //ResultSetFormatter.out(System.out, results, query) ;

            for ( ; results.hasNext() ; )
            {
                QuerySolution soln = results.nextSolution() ;
                // RDFNode x = soln.get("time") ;       // Get a result variable by name.
                //   Resource time = soln.getResource("time") ; // Get a result variable - must be a resource
                Resource property = soln.getResource("dimension");
                Resource machine = soln.getResource("machine");
                Resource model_ = soln.getResource("model");
                Literal cluster = soln.getLiteral("clusters");
                Resource state = soln.getResource("isStateful");
                Literal thresh = soln.getLiteral("threshold");
                // Literal value = soln.getLiteral("value") ;
                //    Literal timeStamp = soln.getLiteral("timeStamp");// Get a result variable - must be a literal


                //  if (value.toString().contains("#string")) {

                //} else {


                str.add(data);
                DebsMetaData db = new DebsMetaData(machine.getLocalName(),model_.getLocalName(),property.getLocalName()
                        ,cluster.getInt(),state.getLocalName(),thresh.getDouble(),50);
                DebsMetaData.storeValues(db);
               // data = machine.getLocalName()+","+model_.getLocalName()+","+property.getLocalName()+","+cluster.getInt()+","+state.getLocalName()+","+thresh.getDouble();
               // System.out.println(data);

                //data = machine.getLocalName()+","+time.getLocalName()+","+timeStamp.getValue()+","+property.getLocalName()+","+value.getFloat();



//                }



                // System.out.println(r);
            }
        }catch(Exception e)
        {
            System.out.print(e);
        }


        System.out.println(DebsMetaData.meta.size());
        for(String key: DebsMetaData.meta.keySet()){
            DebsMetaData dmm = DebsMetaData.meta.get(key);
            System.out.println(dmm.getMachineNumebr()+ " " + dmm.getDimension()+ " " + dmm.getModel()+ " "+ dmm.getClusterCenters()+ " "
            + dmm.getProbabilityThreshold() + " " + dmm.getWindowLength() + " " + dmm.getDimensionState());
        }


        /*for (int i = 0; i<120; i++){
            if(dataArr[i]==null){
                dataArr[i] =0.0F;
            }
            data = data+","+dataArr[i];
        }
        str.add(data);
        File file = new File("src/main/resources/rdfData_extract_100m_time.csv");
        FileWriter writer = null;
        // creates the file
        try {
            file.createNewFile();
            writer = new FileWriter(file);
            for (int i =1; i<str.size(); i++){

                writer.write(str.get(i)+"\n");
            }

            writer.flush();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }*/
    }
}
