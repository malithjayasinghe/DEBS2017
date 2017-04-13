package org.wso2.siddhi.debs2017.input.sparql;

import com.lmax.disruptor.RingBuffer;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.debs2017.processor.EventWrapper;

import java.util.ArrayList;
import java.util.Arrays;
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
*/
public class SorterThread extends Thread {

    public static ArrayList<LinkedBlockingQueue<ObservationGroup>> arrayList;
    private ArrayList<ObservationGroup> arr = new ArrayList<>();
    private ArrayList<Integer> arr2 = new ArrayList<>();
    private static AtomicInteger size;
    private static RingBuffer<EventWrapper> ring;
    static int queNo = -1;
    // private Object[] termination = new A;

    /**
     * The constructor
     *
     * @param arrayList  the array list of link blocking queues
     * @param ringBuffer the ring buffer to publish
     */
    public SorterThread(ArrayList<LinkedBlockingQueue<ObservationGroup>> arrayList, RingBuffer<EventWrapper> ringBuffer) {
        this.arrayList = arrayList;
        size = new AtomicInteger(arrayList.size());
        this.ring = ringBuffer;
        for(int i=0; i<size.get(); i++){
            this.arr2.add(queNo);
            this.arr.add(new ObservationGroup(Long.MAX_VALUE, null));
        }

    }

    public void run() {
        while (true) {
            for (int i = 0; i < size.get(); i++) {
                try {
                    if(arr2.get(i)==-1){
                        ObservationGroup ob =arrayList.get(i).take();
                        System.out.println(ob.getTimestamp());
                        if(ob.getTimestamp()!=-1l){

                            arr.set(i, ob);
                            arr2.set(i, 0);
                        } else {
                            //System.out.println(i+"Termination :"+arrayList.get(i).size()+" "+arrayList.get(i).poll()+" "+arrayList.get(i).peek());
                            arrayList.remove(i);

                            arr.remove(i);
                            arr2.remove(i);
                            i= i-1;
                            size.decrementAndGet();


                        }


                    }



                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
            if(size.get()==0){
                System.out.println("Termination recieved");
                long sequence = this.ring.next();  // Grab the next sequence
                try {
                    EventWrapper wrapper = this.ring.get(sequence); // Get the entry in the Disruptor
                    wrapper.setEvent(new Event(-1l, new Object[]{}));
                } finally {
                    this.ring.publish(sequence);
                }
                break;
            } else {
                sort();
            }

        }
    }




    /**
     * Sort events based on their time stamp
     */
    private synchronized void sort() {
        if (arr.size() > 0) {
            ObservationGroup currentOb = arr.get(0);
            queNo =arr2.get(0);
            for (int i = 1; i < arr.size(); i++) {
                if (currentOb.getTimestamp() > arr.get(i).getTimestamp()) {
                    currentOb = arr.get(i);
                    queNo = i;

                }
            }

            //System.out.println("--"+arr.indexOf(currentOb)+"\t"+currentOb.getTimestamp());


            for (int i =0; i<currentOb.getDataArr().size(); i++){
                long sequence = this.ring.next();  // Grab the next sequence
                try {
                    EventWrapper wrapper = this.ring.get(sequence); // Get the entry in the Disruptor
                    wrapper.setEvent(currentOb.getDataArr().get(i));
                } finally {
                    this.ring.publish(sequence);
                }
            }


            arr2.set(queNo, -1);
            //arr.set(queNo, new ObservationGroup(0l, null));
        }
    }

}