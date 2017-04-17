package org.wso2.siddhi.debs2017.input.sparql;

import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.debs2017.input.UnixConverter;
import org.wso2.siddhi.debs2017.input.metadata.DebsMetaData;
import org.wso2.siddhi.debs2017.query.SingleNodeServer;

import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;

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
public class RegexProcessor  implements Runnable{
    private String data;
    private LinkedBlockingQueue<ObservationGroup> queue;
    private long timestamp;

    @Override
    public void run() {
        ObservationGroup ob;
        ArrayList<Event> arr = new ArrayList<>();
        this.queue = SingleNodeServer.arraylist.get(Integer.parseInt(Thread.currentThread().getName()));

        long timeS = 0l;

        String [] observationGroupArr = data.split("(>)(.)(<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>)(.)(<http://www.agtinternational.com/ontologies/I4.0#MoldingMachineObservationGroup>)");
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
        timeS = UnixConverter.getUnixTime(timeStamp);
        for(int i=1; i<observationArr.length; i++){
            String observation = observationArr[i].split(">")[0];

            String [] propertyArr = observationArr[i].split("(.)(<http://project-hobbit.eu/resources/debs2017#"+observation+">)(.)(<http://purl.oclc.org/NET/ssnx/ssn#observedProperty>)" +
                    "(.)(<http://www.agtinternational.com/ontologies/WeidmullerMetadata#)");

            String property = propertyArr[1].split(">")[0];

            String [] valueArr = propertyArr[1].split("(.)(>)(.)(<http://www.agtinternational.com/ontologies/IoTCore#valueLiteral>)(.)(\")");//9433.11"^^<http://www.w3.org/2001/XMLSchema#double>

            if(!valueArr[1].contains("#string")){
                String value = valueArr[1].split("\"")[0];
                //  System.out.println(machine+"\t"+time+"\t"+timeStamp+"\t"+property+"\t"+value);


                String stateful = property;
                if (DebsMetaData.getMetaData().containsKey(stateful)) {

                    int centers = DebsMetaData.getMetaData().get(stateful).getClusterCenters();
                    double probability = DebsMetaData.getMetaData().get(stateful).getProbabilityThreshold();

                    Event event = new Event(this.timestamp, new Object[]{
                            machine,
                            time,
                            property,
                            timeS,
                            Math.round(Double.parseDouble(value) * 10000.0) / 10000.0, //
                            centers,
                            probability});

                    arr.add(event);

                    //this.queue.put(event);
                }
            }

            //String output = outtputArr[1].split(">")[0];
        }



            ob = new ObservationGroup(timeS, arr);
        try {
            this.queue.put(ob);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }

    public RegexProcessor(String data, long timestamp) {
        this.data = data;
        this.timestamp = timestamp;

    }
}
