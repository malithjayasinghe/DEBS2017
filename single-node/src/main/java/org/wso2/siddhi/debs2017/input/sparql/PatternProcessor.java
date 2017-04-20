package org.wso2.siddhi.debs2017.input.sparql;

import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.debs2017.input.UnixConverter;
import org.wso2.siddhi.debs2017.input.metadata.DebsMetaData;
import org.wso2.siddhi.debs2017.query.SingleNodeServer;

import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
public class PatternProcessor implements Runnable{
    private String data;
    private LinkedBlockingQueue<Event> queue;
    private long timestamp;

    private static Pattern patternTime = Pattern.compile("<http://purl.oclc.org/NET/ssnx/ssn#observationResultTime>.<http://project-hobbit.eu/resources/debs2017#(.*)>");
    private static Pattern patternTimestamp = Pattern.compile("<http://www.agtinternational.com/ontologies/IoTCore#valueLiteral>.\"(.*)\"\\^\\^<http://www.w3.org/2001/XMLSchema#dateTime>");
    private static Pattern patternMachine = Pattern.compile("<http://www.agtinternational.com/ontologies/I4.0#machine>.<http://www.agtinternational.com/ontologies/WeidmullerMetadata#(.*)>");
    private static  Pattern patternProperty = Pattern.compile("<http://purl.oclc.org/NET/ssnx/ssn#observedProperty>.<http://www.agtinternational.com/ontologies/WeidmullerMetadata#(.*)>");
    private static Pattern patternValue = Pattern.compile("<http://project-hobbit.eu/resources/debs2017#Value_.*>.<http://www.agtinternational.com/ontologies/IoTCore#valueLiteral>.\"(.*)\"\\^\\^<http://www.w3.org/2001/XMLSchema#");


    @Override
    public void run() {
        ObservationGroup ob;
        ArrayList<Event> arr = new ArrayList<>();
        this.queue = SingleNodeServer.arraylist.get(Integer.parseInt(Thread.currentThread().getName()));


        String time = "";
        String timeStamp = "";
        String machine = "";
        String property = "";
        String value =  "";


        Matcher matcher1 = patternTime.matcher(this.data);
        while (matcher1.find()) {
            time = matcher1.group(1);
        }

        Matcher matcher2 = patternTimestamp.matcher(this.data);
        while (matcher2.find()) {
            timeStamp = matcher2.group(1);
        }
        long timeS = UnixConverter.getUnixTime(timeStamp);
        Matcher matcher3 = patternMachine.matcher(this.data);
        while (matcher3.find()) {
            machine = matcher3.group(1);
        }

        Matcher matcher4 = patternProperty.matcher(this.data);
        Matcher matcher5 = patternValue.matcher(this.data);

        while (matcher4.find()&& matcher5.find()) {
            //check if stateful
            property = matcher4.group(1);
            value = matcher5.group(1);
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

                //arr.add(event);

                try {
                    this.queue.put(event);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                //this.queue.put(event);
            }

        }

//        ob = new ObservationGroup(timeS, arr);
//        try {
//            this.queue.put(ob);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }



    }








    public PatternProcessor(String data, long timestamp) {
        this.data = data;
        this.timestamp = timestamp;

    }
}

