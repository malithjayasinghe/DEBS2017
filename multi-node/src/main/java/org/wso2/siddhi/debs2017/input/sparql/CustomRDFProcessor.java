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
public class CustomRDFProcessor {

    public void sparql(String rdf){
        String [] observationGroupArr = rdf.split("(>)(.)(<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>)(.)(<http://www.agtinternational.com/ontologies/I4.0#MoldingMachineObservationGroup>)");
        String observationGroup = observationGroupArr[0].split("#")[1];

        String [] timeArr = observationGroupArr[1].split("(<http://project-hobbit.eu/resources/debs2017#"+observationGroup+">)(.)(<http://purl.oclc.org/NET/ssnx/ssn#observationResultTime>)" +
                "(.)(<http://project-hobbit.eu/resources/debs2017#)");
        String time = timeArr[1].split(">")[0];

        String [] machineArr = timeArr[1].split("(.)(<http://project-hobbit.eu/resources/debs2017#"+observationGroup+">)(.)(<http://www.agtinternational.com/ontologies/I4.0#machine>)" +
                "(.)(<http://www.agtinternational.com/ontologies/WeidmullerMetadata#)");
        String machine = machineArr[1].split(">")[0];

        String [] timeStampArr = machineArr[1].split("(.)(<http://project-hobbit.eu/resources/debs2017#"+time+">)(.)(<http://www.agtinternational.com/ontologies/IoTCore#valueLiteral>)" +
                "(.)(\")");
        String timeStamp = timeStampArr[1].split("\"")[0];

        String [] observationArr = timeStampArr[1].split("(.)(<http://project-hobbit.eu/resources/debs2017#"+observationGroup+">)(.)(<http://www.agtinternational.com/ontologies/I4.0#contains>)" +
                "(.)(<http://project-hobbit.eu/resources/debs2017#)");

        for(int i=1; i<observationArr.length; i++){
            String observation = observationArr[i].split(">")[0];

            String [] propertyArr = observationArr[i].split("(.)(<http://project-hobbit.eu/resources/debs2017#"+observation+">)(.)(<http://purl.oclc.org/NET/ssnx/ssn#observedProperty>)" +
                    "(.)(<http://www.agtinternational.com/ontologies/WeidmullerMetadata#)");

            String property = propertyArr[1].split(">")[0];

            String [] valueArr = propertyArr[1].split("(.)(>)(.)(<http://www.agtinternational.com/ontologies/IoTCore#valueLiteral>)(.)(\")");//9433.11"^^<http://www.w3.org/2001/XMLSchema#double>

            if(!valueArr[1].contains("#string")){
                String value = valueArr[1].split("\"")[0];
              //  System.out.println(machine+"\t"+time+"\t"+timeStamp+"\t"+property+"\t"+value);
            }

            //String output = outtputArr[1].split(">")[0];
        }
    }

    public void sparqljena(String message){
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
                "" +
                "";
        try {
            Model model = ModelFactory.createDefaultModel().read(IOUtils.toInputStream(message, "UTF-8"), null, "TURTLE");

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

                   // System.out.println(machine.getLocalName()+"\t"+time.getLocalName()+"\t"+timestamp.getValue()+"\t"+property.getLocalName()+"\t"+value.getFloat());
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
