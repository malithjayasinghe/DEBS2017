package org.wso2.siddhi.debs2017;

import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.debs2017.input.DataPublisher;
import org.wso2.siddhi.debs2017.query.Query;


public class QueryTestCase {

    public static void main(String[] args) {

        QueryTestCase query = new QueryTestCase();
        query.run();
    }

    /**
     * Starts the threads related to Query
     */
    public void run() {

        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "@config(async = 'true') " +
                " define stream inStream (machine string, tstamp string, uTime long, dimension string, " +
                "value double);";

        String query = ("" +
                "\n" +
                "from inStream " +
                "select machine, tstamp, dimension, str:concat(machine, '-', dimension) as partitionId, uTime ,value " +
                "insert into inStreamA;" +
                "\n" +
                "@info(name = 'query1') partition with ( partitionId of inStreamA) " +// perform clustering
                "begin " +
                "from inStreamA#window.externalTime(uTime , 100) \n" +
                "select machine, tstamp, uTime, dimension, debs2017:cluster(value) as center " +
                " insert into #outputStream; " + //inner stream
                "\n" +
                "from #outputStream#window.externalTime(uTime , 100) \n" +
                "select machine, tstamp, dimension, debs2017:markovnew(center) as probability " +
                "insert into detectAnomaly " +
                "end;");


        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);
        executionPlanRuntime.addCallback("detectAnomaly", new StreamCallback() {
            @Override
            public void receive(org.wso2.siddhi.core.event.Event[] events) {
                for (Event ev : events) {
                    // long diff =   System.currentTimeMillis() - (long)ev.getData()[4];
                    //  latency.add(diff);
                    System.out.println(ev.getData()[0] + "," + ev.getData()[1] + "," + ev.getData()[2] + "," + ev.getData()[3]);
                    // System.out.println("Events added"+ " "+ DataPublisher.count +" "+ "Supercount" + " " + DataPublisher.supercount);
                    // System.out.println(latency.size() + " " + "events processed");

                }


            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inStream");
        try {
            inputHandler.send(new Object[]{"m1", "t1", 1483236500l, "d1", 4157.59});
            inputHandler.send(new Object[]{"m1", "t1", 1483236510l, "d1", 4338.76});
            inputHandler.send(new Object[]{"m1", "t1", 1483236520l, "d1", 4563.07});
            inputHandler.send(new Object[]{"m1", "t1", 1483236530l, "d1", 4010.56});
            inputHandler.send(new Object[]{"m1", "t1", 1483236540l, "d1", 4052.87});
            inputHandler.send(new Object[]{"m1", "t1", 1483236550l, "d1", 3831.66});
            inputHandler.send(new Object[]{"m1", "t1", 1483236560l, "d1", 4564.95});
            inputHandler.send(new Object[]{"m1", "t1", 1483236570l, "d1", 3737.3});
            inputHandler.send(new Object[]{"m1", "t1", 1483236580l, "d1", 4808.31});
            inputHandler.send(new Object[]{"m1", "t1", 1483236590l, "d1", 4439.71});
            inputHandler.send(new Object[]{"m1", "t1", 1483236600l, "d1", 4048.21});
            inputHandler.send(new Object[]{"m1", "t1", 1483236610l, "d1", 4536.06});
            inputHandler.send(new Object[]{"m1", "t1", 1483236620l, "d1", 3494.72});
            inputHandler.send(new Object[]{"m1", "t1", 1483236630l, "d1", 4018.0});
            inputHandler.send(new Object[]{"m1", "t1", 1483236640l, "d1", 4031.29});
            inputHandler.send(new Object[]{"m1", "t1", 1483236650l, "d1", 4495.58});
            inputHandler.send(new Object[]{"m1", "t1", 1483236660l, "d1", 3426.6});
            inputHandler.send(new Object[]{"m1", "t1", 1483236670l, "d1", 3454.87});
            inputHandler.send(new Object[]{"m1", "t1", 1483236680l, "d1", 4215.07});
            inputHandler.send(new Object[]{"m1", "t1", 1483236690l, "d1", 3944.41});
        }catch (InterruptedException e){
            System.out.println(e);
        }

        executionPlanRuntime.start();
    }
}
