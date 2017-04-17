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

    public static ArrayList<LinkedBlockingQueue<ObservationGroup>> arrayList;
    private ArrayList<ObservationGroup> unsortedArray = new ArrayList<>();
    private ArrayList<Integer> arrayListStatus = new ArrayList<>();
    private static int size;
    private static RingBuffer<EventWrapper> ring;
    static int queNo = -1;

    /**
     * The constructor
     *
     * @param arrayList  the array list of link blocking queues
     * @param ringBuffer the ring buffer to publish
     */
    public SorterThread(ArrayList<LinkedBlockingQueue<ObservationGroup>> arrayList, RingBuffer<EventWrapper> ringBuffer) {
        this.arrayList = arrayList;
        size = arrayList.size();
        this.ring = ringBuffer;
        for (int i = 0; i < size; i++) {
            this.arrayListStatus.add(queNo);
            this.unsortedArray.add(new ObservationGroup(Long.MAX_VALUE, null));
        }
    }

    /**
     * Perform sorting
     */
    public void run() {
        try {
            while (true) {
                for (int i = 0; i < size; i++) {
                    if (arrayListStatus.get(i) == -1) {
                        ObservationGroup ob = arrayList.get(i).take();
                        if (ob.getTimestamp() != -1l) {
                            unsortedArray.set(i, ob);
                            arrayListStatus.set(i, 0);
                        } else {
                            arrayList.remove(i);
                            unsortedArray.remove(i);
                            arrayListStatus.remove(i);
                            i = i - 1;
                            size--;
                        }
                    }
                }
                if (size == 0) {
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

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * Sort events based on their time stamp
     */
    private void sort() {
            ObservationGroup currentOb = unsortedArray.get(0);
            queNo = arrayListStatus.get(0);
            for (int i = 1; i < unsortedArray.size(); i++) {
                if (currentOb.getTimestamp() > unsortedArray.get(i).getTimestamp()) {
                    currentOb = unsortedArray.get(i);
                    queNo = i;
                }
            }
            for (int i = 0; i < currentOb.getDataArr().size(); i++) {
                long sequence = this.ring.next();  // Grab the next sequence
                try {
                    EventWrapper wrapper = this.ring.get(sequence); // Get the entry in the Disruptor
                    wrapper.setEvent(currentOb.getDataArr().get(i));
                } finally {
                    this.ring.publish(sequence);
                }
            }
            arrayListStatus.set(queNo, -1);
        }

}