package org.wso2.siddhi.debs2017.processor;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;

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
public class SiddhiEventHandler implements EventHandler<EventWrapper> {

    private final long ID;
    private final long NUM;
    private final SiddhiQuery sq;


    @Override
    public void onEvent (EventWrapper wrapper , long sequence, boolean b) throws Exception {


        Object[] o = wrapper.getEvent().getData();
        if(wrapper.getEvent().getTimestamp() == -1l){

        } else {
            String[] splitter = o[2].toString().split("_");
            long partition = Long.parseLong(splitter[2]);
            //long partition = Long.parseLong(o[2].toString().substring(1));
            if (partition % NUM == ID) {

                //setting the buffer sequence
                sq.setSequence(sequence);


                sq.publish(wrapper.getEvent());
            }
        }

    }

    public SiddhiEventHandler(long id, long num, RingBuffer<EventWrapper> ringBuffer){
        this.ID = id;
        this.NUM = num;
        this.sq = new SiddhiQuery(ringBuffer);
    }




}

