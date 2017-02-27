package org.wso2.siddhi.debs2017.processor;

import com.lmax.disruptor.RingBuffer;

import java.util.ArrayList;

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
public class DebsEventProducer  {

    private final RingBuffer<DebsEvent> ringBuffer;
   // public static int count;

    public DebsEventProducer(RingBuffer<DebsEvent> ringBuffer)
    {
        this.ringBuffer = ringBuffer;
    }

    public void onData(String machine, String tStamp, long uTime, String dimension, double value, long ij_time)
    {
        long sequence = ringBuffer.next();  // Grab the next sequence
        try
        {
            DebsEvent event = ringBuffer.get(sequence); // Get the entry in the Disruptor
            event.setMachine(machine);
            event.settStamp(tStamp);
            event.setuTime(uTime);
            event.setDimension(dimension);
            event.setValue(value);
            event.setIj_time(ij_time);
            // for the sequence

        }
        finally
        {

            ringBuffer.publish(sequence);
            if (ringBuffer.isPublished(sequence)){
                //System.out.println("Seq published\t");
               // count++;
                // System.exit(0);
            }/*else{
                System.out.println("\n -----------------------------------not published ----------------------------");
                System.exit(0);
            }*/
        }
    }
}
