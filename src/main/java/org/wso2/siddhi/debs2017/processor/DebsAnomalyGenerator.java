package org.wso2.siddhi.debs2017.processor;

import com.lmax.disruptor.EventHandler;
import org.wso2.siddhi.debs2017.Output.AlertGenerator;
import org.wso2.siddhi.debs2017.input.DebsDataPublisher;
import org.wso2.siddhi.debs2017.query.DistributedQuery;

import java.util.ArrayList;

/**
 * Created by sachini on 3/2/17.
 */
public class DebsAnomalyGenerator implements EventHandler<DebsEvent> {
    private static ArrayList<Long> arr = new ArrayList<>();
    @Override
    public void onEvent(DebsEvent debsEvent, long l, boolean b) throws Exception {

        addToArray(System.currentTimeMillis()-debsEvent.getIj_time());


            if (debsEvent.getProbability() < 0.3 && debsEvent.getProbability()>=0) {
                System.out.println(" Machine" + debsEvent.getMachine() + "\t" + "Timestamp" + debsEvent.gettStamp() +
                        "\t" + "Dimension" + debsEvent.getDimension() + "\t" + "Anomaly" + debsEvent.getProbability());

                debsEvent.setProbThresh("0.5");
                AlertGenerator ag = new AlertGenerator(debsEvent);
                ag.generateAlert();

            }


        if(arr.size() == DebsDataPublisher.superCount){
            long endtime = System.currentTimeMillis();
            System.out.println("endtime"+endtime);
            long totaltime =(endtime- DistributedQuery.starttime)/1000;
            System.out.println("\nTotaltime:" +(totaltime));
            System.out.println("Throughput:"+(arr.size()/totaltime));
            long sum =0;
            for(int i =0; i<arr.size(); i++){
                sum =  sum + arr.get(i);
            }
            System.out.println("Total Lat:"+(sum));
            System.out.println("Avg Lat:"+(sum/arr.size()));
            System.out.println("Data:"+arr.size());
        }

    }

    public static synchronized void addToArray(Long diff)
    {
        arr.add(diff);

    }

}
