package org.wso2.siddhi.debs2017.query;

import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;


import java.io.File;

/**
 * Created by temp on 1/25/17.
 */
public class Query {


    /**
     * The constructor
     *
     * @param args arguments
     */
    private Query(String[] args){

    }
    private Query(){}

    /**
     * The main method
     *
     * @param args arguments
     */
    public static void main(String[] args){

        File q1 = new File("100m_extract.csv");
        q1.delete();


        /*if(args.length == 0){
            System.err.println("Incorrect arguments. Required: <Path to>extract.dat");
            return;
        }*/
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
                "from inStreamA#window.length(3)" +
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

        //@ Sachini : Handle DataLoader here
        executionPlanRuntime.start();
        try {
            System.out.println("---------------------test");
            inputHandler.send(new Object[]{"machine1","tsamp", "dimension",1.0});
            inputHandler.send(new Object[]{"machine1","tsamp", "dimension",2.0});
            inputHandler.send(new Object[]{"machine2","tsamp", "dimension",3.0});
            inputHandler.send(new Object[]{"machine2","tsamp", "dimension",4.0});
            inputHandler.send(new Object[]{"machine1","tsamp", "dimension",5.0});
            inputHandler.send(new Object[]{"machine1","tsamp", "dimension",6.0});
            inputHandler.send(new Object[]{"machine3","tsamp", "dimension",7.0});
            inputHandler.send(new Object[]{"machine1","tsamp", "dimension",8.0});
            inputHandler.send(new Object[]{"machine2","tsamp", "dimension",9.0});
            inputHandler.send(new Object[]{"machine2","tsamp", "dimension",10.0});



            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }




    }

}
