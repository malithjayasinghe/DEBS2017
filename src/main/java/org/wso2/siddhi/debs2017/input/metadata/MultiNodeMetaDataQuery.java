package org.wso2.siddhi.debs2017.input.metadata;

import com.hp.hpl.jena.query.*;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;
import org.apache.jena.riot.RDFDataMgr;

/**
 * Created by sachini on 4/3/17.
 */
public class MultiNodeMetaDataQuery {
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
                for(int i = 0; i<500; i++) {
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
                    String dimension = property.getLocalName().replace("_59","_"+i);
                    DebsMetaData db = new DebsMetaData("Machine_" + i, dimension
                            , cluster.getInt(), thresh.getDouble());
                    DebsMetaData.storeValues(db);
                }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }




}
