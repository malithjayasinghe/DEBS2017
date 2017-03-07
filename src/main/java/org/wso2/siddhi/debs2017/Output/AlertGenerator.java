package org.wso2.siddhi.debs2017.Output;

/**
 * Created by sachini on 2/6/17.
 */

import com.hp.hpl.jena.rdf.model.*;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.wso2.siddhi.debs2017.processor.DebsEvent;
import java.util.ArrayList;


/**
 * class to generate an alert when anomaly detected
 * an rdf model is created
 */
public class AlertGenerator {
    private static int anomalyCount = 1;
    private String probThresh;
    private String timestamp;
    private String dimension;
    private String machineNumber;
    private String timestampValue;
    private ArrayList<Model> alert= new ArrayList<>();
    String anomaly = "http://wso2.org.debsgrandchallenge.anomaly#";
    String ar = "http://www.agtinternational.com/ontologies/DEBSAnalyticResults#";
    String rdf = "http://www.agtinternational.com/ontologies/DEBSAnalyticResults#";
    String debs = "http://project-hobbit.eu/resources/debs2017#";
    String xsd = "http://www.w3.org/2001/XMLSchema#";
    String wmm = "http://www.agtinternational.com/ontologies/WeidmullerMetadata#";
    String  i40 = "http://www.agtinternational.com/ontologies/I4.0#";
    String IoTCore = "http://www.agtinternational.com/ontologies/IoTCore#";



    public  AlertGenerator(DebsEvent dout){
            this.probThresh = dout.getProbThresh();
            this.timestamp = dout.gettStamp();
            this.dimension = dout.getDimension();
            this.machineNumber = dout.getMachine();
            this.timestampValue = dout.getSentTime();
    }
    public void generateAlert(){

      Model model = ModelFactory.createDefaultModel();
       String anomalyName = "Anomaly_" + anomalyCount;
       Resource r1 = model.createResource(anomaly + anomalyName);
        Resource timeStampRes = model.createResource(anomaly+ timestamp);
        Property type = model.createProperty(rdf + "type");
        Resource r2 = model.createResource(ar + "Anomaly");
        Property threshProb = model.createProperty(ar + "hasProbabilityOfObservedAbnormalSequence");
        Resource r3 = model.createResource(probThresh);
        Property time = model.createProperty(ar + "hasTimestamp");
        Resource r4 = model.createResource(debs + timestamp);
        Property dim = model.createProperty(ar + "inAbnormalDimension");
        Resource r5 = model.createResource(wmm + dimension);
        Property machine = model.createProperty(i40 + "machine" );
        Resource r6 = model.createResource(wmm + machineNumber);
        Resource timeType = model.createResource(IoTCore + "Timestamp");
        Property timeValue = model.createProperty(IoTCore + "valueLiteral");
        Resource currentTime = model.createResource(timestampValue);


        model.add(r1,type,r2)
                .add(r1,threshProb,r3)
        .add(r1,time,r4)
        .add(r1,dim,r5)
        .add(r1,machine,r6)
        .add(timeStampRes,type,timeType)
        .add(timeStampRes,timeValue,currentTime);

        //alert.add(model);






       //RDFDataMgr.write(System.out, model, RDFFormat.TURTLE_FLAT) ;
       System.out.println("Anomaly"+ machineNumber + " "+ timestamp + " " + dimension);


    }
}
