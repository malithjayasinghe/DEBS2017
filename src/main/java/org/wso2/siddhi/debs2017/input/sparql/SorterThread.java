package org.wso2.siddhi.debs2017.input.sparql;


import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.debs2017.transport.TcpNettyClient;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
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
public class SorterThread extends Thread {

    public static ArrayList<LinkedBlockingQueue<Event>> arrayList;
    private ArrayList<Event> arr = new ArrayList<>();
    private ArrayList<Integer> arr2 = new ArrayList<>();
    private static int size;
    private static int count =0;
    private TcpNettyClient siddhiClient = new TcpNettyClient();
    private TcpNettyClient siddhiClient1 = new TcpNettyClient();
    private TcpNettyClient siddhiClient2 = new TcpNettyClient();

    File file = new File("/home/fathima/stuff4.txt");
    FileWriter writer = null;

    public SorterThread(ArrayList<LinkedBlockingQueue<Event>> arrayList, String host1, int port1, String host2, int port2, String host3, int port3){
        this.arrayList = arrayList;
        this.size = arrayList.size();
        this.siddhiClient.connect(host1, port1);
        this.siddhiClient1.connect(host2, port2);
        this.siddhiClient2.connect(host3, port3);

        try {
            file.createNewFile();
            writer = new FileWriter(file);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void run(){
        while (true){

            for(int i=0; i<this.size; i++){
                try {
                    if(arrayList.get(i).size()!=0){
                        arr.add(arrayList.get(i).peek());
                        arr2.add(i);
                    }
                    else {

                        long timeout = System.currentTimeMillis();
                        while(true){

                            if((System.currentTimeMillis() - timeout)>=5){
                                break;
                            }
                            if(arrayList.get(i).size()!=0){
                                arr.add(arrayList.get(i).peek());
                                arr2.add(i);
                                break;
                            }
                        }

                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            sort();
        }
    }

    private int getTime(Event event) {
        String time = (String) event.getData()[1];
        int timestamp = Integer.parseInt(time.substring(10));
        return timestamp;
    }


    private void removeEvent(Event e, int queNo) {


        int machineNo = Integer.parseInt(e.getData()[0].toString().substring(15));

            LinkedBlockingQueue<Event> linkedBlockingQueue = arrayList.get(queNo);
            if (machineNo % 3 == 0) {
                linkedBlockingQueue.poll();
                e.getData()[7] =0 ;
                siddhiClient.send("input", new Event[]{e});
            } else if (machineNo % 3 == 1) {
                e.getData()[7] =1 ;
                linkedBlockingQueue.poll();
                siddhiClient1.send("input", new Event[]{e});
            } else if (machineNo % 3 % 3 == 2) {
                e.getData()[7] =2;
                linkedBlockingQueue.poll();
                siddhiClient2.send("input", new Event[]{e});
            }
        count++;

        System.out.println(count+"\t"+e);
        try {
            writer.write(count+"\t"+e+"\n");
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        if(count%270==0){
            System.out.println("\n\n===========================================");
            try {
                writer.write("\n\n===========================================");
            } catch (IOException e1) {
                e1.printStackTrace();
            }
        }
        if(count==172530){

            try {
                writer.flush();
                writer.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
        }
    }

    private synchronized void sort() {

        if (arr.size() > 0) {
            Event currentEvent = arr.get(0);
            Integer queNo = arr2.get(0);

            for (int i = 1; i < arr.size(); i++) {
                if (getTime(currentEvent) > getTime(arr.get(i))){
                    currentEvent = arr.get(i);
                    queNo = arr2.get(i);
                }
            }

            arr = new ArrayList<>();
            arr2 = new ArrayList<>();
            removeEvent(currentEvent, queNo);

        }

    }


    
}
