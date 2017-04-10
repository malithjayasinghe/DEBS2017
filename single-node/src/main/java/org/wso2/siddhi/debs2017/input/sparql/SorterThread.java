package org.wso2.siddhi.debs2017.input.sparql;

import com.lmax.disruptor.RingBuffer;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.debs2017.processor.EventWrapper;

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
    private static RingBuffer<EventWrapper> ring;

    /**
     * The constructor
     *
     * @param arrayList  the array list of link blocking queues
     * @param ringBuffer the ring buffer to publish
     */
    public SorterThread(ArrayList<LinkedBlockingQueue<Event>> arrayList, RingBuffer<EventWrapper> ringBuffer) {
        this.arrayList = arrayList;
        this.size = arrayList.size();
        this.ring = ringBuffer;
    }

    public void run() {
        while (true) {
            for (int i = 0; i < this.size; i++) {
                try {
                    if (arrayList.get(i).size() != 0) {
                        arr.add(arrayList.get(i).peek());
                        arr2.add(i);
                    } else {
                        long timeout = System.currentTimeMillis();
                        while (true) {
                            if ((System.currentTimeMillis() - timeout) >= 3000) {
                                break;
                            }
                            if (arrayList.get(i).size() != 0) {
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

    /**
     * Gets the time stamp from the event
     *
     * @param event the event to get the time stamp from
     * @return the time stamp
     */
    private int getTime(Event event) {
        String time = (String) event.getData()[1];
        int timestamp = Integer.parseInt(time.substring(10));
        return timestamp;
    }

    /**
     * Remove event from the blocking queue
     *
     * @param e     event to be removed
     * @param queNo blocking queue number
     */
    private void removeEvent(Event e, int queNo) {
        //Machine_59
        //MoldingMachine_59
        int machineNo = Integer.parseInt(e.getData()[0].toString().substring(8));
        LinkedBlockingQueue<Event> linkedBlockingQueue = arrayList.get(queNo);
        //add to disruptor
        long sequence = this.ring.next();  // Grab the next sequence
        try {
            EventWrapper wrapper = this.ring.get(sequence); // Get the entry in the Disruptor
            wrapper.setEvent(linkedBlockingQueue.poll());
        } finally {
            this.ring.publish(sequence);
        }
    }

    /**
     * Sort events based on their time stamp
     */
    private synchronized void sort() {
        if (arr.size() > 0) {
            Event currentEvent = arr.get(0);
            Integer queNo = arr2.get(0);
            for (int i = 1; i < arr.size(); i++) {
                if (getTime(currentEvent) > getTime(arr.get(i))) {
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
