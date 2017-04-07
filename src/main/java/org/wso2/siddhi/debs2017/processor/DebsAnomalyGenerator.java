/*
 *
 *  Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * /
 *
 */

package org.wso2.siddhi.debs2017.processor;

import com.lmax.disruptor.EventHandler;
import org.wso2.siddhi.debs2017.output.AlertGenerator;
import org.wso2.siddhi.debs2017.input.DebsDataPublisher;
import org.wso2.siddhi.debs2017.query.DistributedQuery;

import java.util.ArrayList;
/**
@deprecated
*/

public class DebsAnomalyGenerator implements EventHandler<DebsEvent> {
    private static ArrayList<Long> arr = new ArrayList<>();

    @Override
    public void onEvent(DebsEvent debsEvent, long l, boolean b) throws Exception {


        addToArray(System.currentTimeMillis() - debsEvent.getIj_time());


        if (debsEvent.getProbability() < 0.3 && debsEvent.getProbability() >= 0) {
          // System.out.println(" Machine" + debsEvent.getMachine() + "\t" + "Timestamp" + debsEvent.gettStamp() +
            //      "\t" + "Dimension" + debsEvent.getDimension() + "\t" + "Anomaly" + debsEvent.getProbability());
            debsEvent.setProbThresh("0.5");
            AlertGenerator ag = new AlertGenerator(debsEvent);
            ag.generateAlert();

        }


        if (arr.size() == DebsDataPublisher.superCount) {
            long endtime = System.currentTimeMillis();
            System.out.println("endtime" + endtime);
            long totaltime = (endtime - DistributedQuery.starttime) / 1000;
            System.out.println("\nTotaltime:" + (totaltime));
            System.out.println("Throughput:" + (arr.size() / totaltime));
            double sum = 0;
            for (int i = 0; i < arr.size(); i++) {
                sum = sum + arr.get(i);
            }
            System.out.println("Total Lat:" + (sum));
            System.out.println("Avg Lat:" + (sum / arr.size()));
            System.out.println("Data:" + arr.size());
        }

    }

    public static synchronized void addToArray(Long diff) {
        arr.add(diff);

    }

}
