package org.wso2.siddhi.debs2017.input.sparql;

import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.debs2017.input.UnixConverter;
import org.wso2.siddhi.debs2017.input.metadata.DebsMetaData;
import org.wso2.siddhi.debs2017.query.SingleNodeServer;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
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
public class LineProcessor implements Runnable {
    private final long timestamp;
    private byte [] data;
    private LinkedBlockingQueue<Event> queue;
    static Pattern patternTime = Pattern.compile("ationResultTime>.<http://project-hobbit.eu/resources/debs2017#(\\w*)>");
    static Pattern patternTimestamp = Pattern.compile("valueLiteral>.\"(.*)\"\\^");
    static Pattern patternMachine = Pattern.compile("WeidmullerMetadata#(.*)>");
    static  Pattern patternProperty = Pattern.compile("WeidmullerMetadata#(\\w*)>");
    static Pattern patternValue = Pattern.compile("debs2017#Value_.*>.<http://www.agtinternational.com/ontologies/IoTCore#valueLiteral>.\"(.*)\"\\^\\^<http://www.w3.org/2001/XMLSchema#");


     InputStream is = null;
     BufferedReader bfReader = null;
    @Override
    public void run() {

        this.queue = SingleNodeServer.arraylist.get(Integer.parseInt(Thread.currentThread().getName()));
        int count = 0;
        int propCount = 12;
        int valCount = 16;
        int nextOccurrence = 8;

        String time = "";
        String timeStamp = "";
        String machine = "";
        String property = "";
        String value =  "";
        long uTime = 0l;
        try {
            is = new ByteArrayInputStream(data);
            bfReader = new BufferedReader(new InputStreamReader(is));
            String temp = null;
            while((temp = bfReader.readLine()) != null){
                count++;
                if(count==2){
                    Matcher matcher1 = patternTime.matcher(temp);
                    while (matcher1.find()) {
                      //  System.out.println("----------------||-time");
                        time = matcher1.group(1);
                    }

                } else  if(count==3) {
                    Matcher matcher3 = patternMachine.matcher(temp);
                    while (matcher3.find()) {
                      //  System.out.println("-----------------machine");
                        machine = matcher3.group(1);
                    }

                } else  if(count==8) {
                    //System.out.println(count+"\t"+temp);
                    Matcher matcher2 = patternTimestamp.matcher(temp);
                    while (matcher2.find()) {
                        timeStamp = matcher2.group(1);
                       // System.out.println(timeStamp + "\t"+timeStamp.length());
                        uTime = UnixConverter.getUnixTime(timeStamp);
                    }
                } else if (count==propCount){
                    // System.out.println(temp);
                    propCount+=nextOccurrence;
                    Matcher matcher4 = patternProperty.matcher(temp);

                    while (matcher4.find()) {
                        property = matcher4.group(1);

                    }

                }else if (count==valCount){
                    // System.out.println(temp);
                    valCount+=nextOccurrence;
                    Matcher matcher5 = patternValue.matcher(temp);

                    while (matcher5.find()) {

                        value = matcher5.group(1);
                        String stateful = property;
                        if (DebsMetaData.getMetaData().containsKey(stateful)) {

                            int centers = DebsMetaData.getMetaData().get(stateful).getClusterCenters();
                            double probability = DebsMetaData.getMetaData().get(stateful).getProbabilityThreshold();

                            Event event = new Event(this.timestamp, new Object[]{
                                    machine,
                                    time,
                                    property,
                                    uTime,
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

                }
              //  System.out.println(count+"\t"+temp);

            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try{
                if(is != null) is.close();
            } catch (Exception ex){

            }
        }


    }
    
    public LineProcessor(byte[] data, long timestamp) {
        this.data = data;
        this.timestamp = timestamp;

    
        
    }
}
