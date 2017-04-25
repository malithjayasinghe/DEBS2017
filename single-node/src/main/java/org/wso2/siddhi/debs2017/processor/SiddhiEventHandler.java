package org.wso2.siddhi.debs2017.processor;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.debs2017.input.sparql.RabbitMessage;

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

/**
 * SiddhiEventHandler
 */
public class SiddhiEventHandler implements EventHandler<RabbitMessage> {

    private static final Logger logger = LoggerFactory.getLogger(SiddhiEventHandler.class);
    private final int id;
    private final int num;
    private final SiddhiQuery sq;


    @Override
    public void onEvent(RabbitMessage message, long sequence, boolean b) throws Exception {


        if (message.isStateful()) {
            Object[] o = message.getEvent().getData();
            if (message.getEvent().getTimestamp() == -1L) {

            } else {

                String[] splitter = o[2].toString().split("_");
                long partition = Long.parseLong(splitter[2]);

                if (partition % num == id) {

                    //setting the buffer sequence
                    sq.setSequence(sequence);

                    //publish event to sidhdhi
                    sq.publish(message.getEvent());
                }
            }
        }
    }

    public SiddhiEventHandler(int id, int num, RingBuffer<RabbitMessage> ringBuffer) {
        this.id = id;
        this.num = num;
        this.sq = new SiddhiQuery(ringBuffer);
        //this.alertGenerator = alertGenerator;

    }


}

