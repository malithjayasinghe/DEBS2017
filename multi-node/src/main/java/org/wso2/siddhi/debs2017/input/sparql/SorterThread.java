package org.wso2.siddhi.debs2017.input.sparql;


import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.debs2017.transport.utils.TcpNettyClient;

import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

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
*
* Sorts the output at the dispatcher and publishes the event to siddhi workers
*
*/
public class SorterThread extends Thread {

    public static ArrayList<LinkedBlockingQueue<ObservationGroup>> arrayList;
    private ArrayList<ObservationGroup> arr = new ArrayList<>();
    private ArrayList<Integer> arr2 = new ArrayList<>();
    private static AtomicInteger size;
    private TcpNettyClient siddhiClient0 = new TcpNettyClient();
    private TcpNettyClient siddhiClient1 = new TcpNettyClient();
    private TcpNettyClient siddhiClient2 = new TcpNettyClient();
    static int queNo = -1;

    /**
     * The construntor
     *
     * @param arrayList : list of linked blocking queues
     * @param host1     : host of siddhi worker 1
     * @param port1     : port of siddhi worker 1
     * @param host2     : host of siddhi worker 2
     * @param port2     : port of siddhi worker 2
     * @param host3     : host of siddhi worker 3
     * @param port3     : port of siddhi worker 3
     */

    public SorterThread(ArrayList<LinkedBlockingQueue<ObservationGroup>> arrayList, String host1, int port1, String host2, int port2, String host3, int port3) {
        this.arrayList = arrayList;
        this.size = new AtomicInteger(arrayList.size());
        this.siddhiClient0.connect(host1, port1);
        this.siddhiClient1.connect(host2, port2);
        this.siddhiClient2.connect(host3, port3);
        for (int i = 0; i < size.get(); i++) {
            this.arr2.add(queNo);
            this.arr.add(new ObservationGroup(Long.MAX_VALUE, null));
        }
    }

    /**
     * continuously checks if any event is added to queue, if so peeks to a temporary list
     */
//    public void run() {
//        while (true) {
//            for (int i = 0; i < this.size; i++) {
//                try {
//                    if (arrayList.get(i).size() != 0) {
//                        arr.add(arrayList.get(i).peek());
//                        arr2.add(i);
//                    } else {
//                        long timeout = System.currentTimeMillis();
//                        while (true) {
//                            if ((System.currentTimeMillis() - timeout) >= 3000) {
//                                break;
//                            }
//                            if (arrayList.get(i).size() != 0) {
//                                arr.add(arrayList.get(i).peek());
//                                arr2.add(i);
//                                break;
//                            }
//                        }
//
//                    }
//
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            }
//            sort();
//        }
//    }
    public void run() {
        while (true) {
            for (int i = 0; i < size.get(); i++) {
                try {
                    if (arr2.get(i) == -1) {
                        ObservationGroup ob = arrayList.get(i).take();

                        if (ob.getTimestamp() != -1l) {

                            arr.set(i, ob);
                            arr2.set(i, 0);
                        } else {
                            //System.out.println(i+"Termination :"+arrayList.get(i).size()+" "+arrayList.get(i).poll()+" "+arrayList.get(i).peek());
                            arrayList.remove(i);

                            arr.remove(i);
                            arr2.remove(i);
                            i = i - 1;
                            size.decrementAndGet();
                        }


                    }


                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
            if (size.get() == 0) {

                //publish to all nodes

                Event e1 = new Event(-1l, new Object[]{"machine", "time", "dimension", -1L, -1.0, 0, -1.0, 0});
                Event e2 = new Event(-1l, new Object[]{"machine", "time", "dimension", -1L, -1.0, 1, -1.0, 1});
                Event e3 = new Event(-1l, new Object[]{"machine", "time", "dimension", -1L, -1.0, 2, -1.0, 2});

                System.out.println("sorter - Terminated");
                siddhiClient0.send("input", new Event[]{e1});
                siddhiClient1.send("input", new Event[]{e2});
                siddhiClient2.send("input", new Event[]{e3});
                /*
                attribute("machine", Attribute.Type.STRING).
                    attribute("time", Attribute.Type.STRING).
                    attribute("dimension", Attribute.Type.STRING).
                    attribute("uTime", Attribute.Type.LONG).
                    attribute("value", Attribute.Type.DOUBLE).
                    attribute("centers", Attribute.Type.INT).
                    attribute("threshold", Attribute.Type.DOUBLE).
                    attribute("node", Attribute.Type.INT);
                 */


                break;
            } else {
                sort();
            }

        }
    }

    /**
     * Takes in the event, substrings the numerical part of the timestamp and returns it as a integer
     *
     * @param event : event
     * @return : integer output of the numerical value
     */
    private int getTime(Event event) {
        String time = (String) event.getData()[1];
        int timestamp = Integer.parseInt(time.substring(10));
        return timestamp;
    }


    /**
     * gets the event with smallest timestamp of the temporary list, which holds the first event of each queue
     */
    private synchronized void sort() {
        if (arr.size() > 0) {
            ObservationGroup currentOb = arr.get(0);
            queNo = arr2.get(0);
            for (int i = 1; i < arr.size(); i++) {
                if (currentOb.getTimestamp() > arr.get(i).getTimestamp()) {
                    currentOb = arr.get(i);
                    queNo = i;

                }
            }

            //System.out.println("--"+arr.indexOf(currentOb)+"\t"+currentOb.getTimestamp());


            for (int i = 0; i < currentOb.getDataArr().size(); i++) {
                Event e = currentOb.getDataArr().get(i);
                System.out.println(e);
                long machineNo = Long.parseLong(e.getData()[0].toString().split("_")[1]);
                if (machineNo % 3 == 0) {
                    e.getData()[7] = 0;
                    siddhiClient0.send("input", new Event[]{e});
                } else if (machineNo % 3 == 1) {
                    e.getData()[7] = 1;

                    siddhiClient1.send("input", new Event[]{e});
                } else if (machineNo % 3 == 2) {
                    e.getData()[7] = 2;
                    siddhiClient2.send("input", new Event[]{e});
                }


            }


            arr2.set(queNo, -1);
            //arr.set(queNo, new ObservationGroup(0l, null));
        }
    }
}


