package org.wso2.siddhi.debs2017.query;

import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.debs2017.Output.AlertGenerator;
import org.wso2.siddhi.debs2017.input.DataPublisher;

/**
 * Created by temp on 1/25/17.
 */
public class Query {

    private Query() {
    }

    /**
     * The main method
     *
     * @param args arguments
     */
    public static void main(String[] args) {

        Query query = new Query();
        query.run();
    }

    /**
     * Starts the threads related to Query
     */
    public void run() {

        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "@config(async = 'true') \n" +
                "define stream inStream (machine string, tstamp string, uTime long, dimension string, " +
                "value double);";

        String query = ("" +
                "\n" +
                "from inStream " +
                "select machine, tstamp, dimension, str:concat(machine, '-', dimension) as partitionId, uTime ,value " +
                "insert into inStreamA;" +
                "\n" +
                "@info(name = 'query1') partition with ( partitionId of inStreamA) " +// perform clustering
                "begin " +
                "from inStreamA#window.externalTime(uTime , 50) \n" +
                "select machine, tstamp, uTime, dimension, debs2017:cluster(value) as center" +
                " insert into #outputStream; " + //inner stream
                "\n" +
                "from #outputStream#window.externalTime(uTime , 50) " +
                "select machine, tstamp, dimension, debs2017:markov(center) as probability " +
                "insert into detectAnomaly " +
                "end;");


        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);
        executionPlanRuntime.addCallback("detectAnomaly", new StreamCallback() {
            @Override
            public void receive(org.wso2.siddhi.core.event.Event[] events) {
                for (Event ev : events) {
                    System.out.println(ev.getData()[0] + "," + ev.getData()[1] + "," + ev.getData()[2] + "," + ev.getData()[3]);

                }

            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inStream");
        DataPublisher dataPublisher = new DataPublisher("rdfData_extract_100m_time.csv", inputHandler);
        executionPlanRuntime.start();
        dataPublisher.publish();

        while (true) {
            try {
                Thread.currentThread().sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


    }

}
