package org.wso2.siddhi.debs2017.processor;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import org.wso2.siddhi.debs2017.input.sparql.RabbitMessage;
import org.wso2.siddhi.debs2017.output.AlertGenerator;

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
public class SiddhiEventHandler implements EventHandler<RabbitMessage> {

    private final int ID;
    private final int NUM;
    private final SiddhiQuery sq;
    private RingBuffer<RabbitMessage> ringBuffer;
    private static AlertGenerator alertGenerator;


    @Override
    public void onEvent (RabbitMessage message , long sequence, boolean b) throws Exception {
       // System.out.println(sequence + "Sequence accessed by sidhhi");
       // System.out.println(message.getEvent()+ "Sidhhi handler------------");
        if(message.isStateful()) {
            Object[] o = message.getEvent().getData();
            if (message.getEvent().getTimestamp() == -1l) {
                //System.out.println(("Termination received by sidhhi handler"));
               // alertGenerator.terminate();
            } else {

                String[] splitter = o[2].toString().split("_");
                long partition = Long.parseLong(splitter[2]);

               if (partition % NUM == ID) {

                    //setting the buffer sequence
                    sq.setSequence(sequence);

                    //publish event to sidhdhi
                    sq.publish(message.getEvent());
               }
            }
        }
    }

    public SiddhiEventHandler(int id, int num, RingBuffer<RabbitMessage> ringBuffer){
        this.ID = id;
        this.NUM = num;
        this.sq = new SiddhiQuery(ringBuffer,alertGenerator);
        this.ringBuffer = ringBuffer;
        //this.alertGenerator = alertGenerator;

    }




}

