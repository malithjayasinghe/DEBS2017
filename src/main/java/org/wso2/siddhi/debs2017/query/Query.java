package org.wso2.siddhi.debs2017.query;

import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.debs2017.input.DataPublisher;


import java.io.File;

/**
 * Created by temp on 1/25/17.
 */
public class Query {




    private Query(){}

    /**
     * The main method
     *
     * @param args arguments
     */
    public static void main(String[] args){

        Query query = new Query();
        query.run();
    }

    /**
     * Starts the threads related to Query
     */
    public void run() {


        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "@config(async = 'true') \n" +
                "define stream inStream (machine string, tstamp string, dimension string, " +
                "value double);";

        String query = ("" +
                "\n" +
                "from inStream " +
                "select machine, tstamp, dimension, str:concat(machine, '-', dimension) as partitionId, value " +
                "insert into inStreamA;" +
                "\n" +
                "@info(name = 'query1') partition with ( partitionId of inStreamA) " +
                "begin " +
                "from inStreamA#window.length(10)" +
                "select machine, tstamp, dimension, debs2017:cluster(value) as center" +
                " insert into outputStream " +
                "end;");



        System.out.println(inStreamDefinition + query);
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(org.wso2.siddhi.core.event.Event[] events) {
                // EventPrinter.print(events);
                for(Event ev : events){
                    System.out.println(ev.getData()[0]+"\t"+ ev.getData()[1]+"\t"+ ev.getData()[2]+"\t"+ev.getData()[3]);


                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inStream");
        DataPublisher dataPublisher = new DataPublisher("100m_extract.csv",inputHandler);
        executionPlanRuntime.start();
        //the data is passed as objects to the inputhandler
        dataPublisher.publish();









    }

}
