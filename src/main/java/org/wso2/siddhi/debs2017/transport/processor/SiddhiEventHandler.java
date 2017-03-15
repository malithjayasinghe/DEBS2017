package org.wso2.siddhi.debs2017.transport.processor;


import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.debs2017.transport.processor.SiddhiQuery;

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
public class SiddhiEventHandler implements EventHandler<Event> {

    private final long ID;
    private final long NUM;
    private final SiddhiQuery sq;


    @Override
    public void onEvent (Event event , long sequence, boolean b) throws Exception {
        Object[] o = event.getData();
        long partition = Long.parseLong(o[4].toString().substring(1));
        if(partition%NUM==ID){

            //setting the event to be altered
            sq.setEvent(event);
            //setting the buffer sequence
            sq.setSequence(sequence);


            sq.publish(event);
        }

    }


    public SiddhiEventHandler(long id, long num, RingBuffer<Event> ringBuffer){
        this.ID = id;
        this.NUM = num;
        this.sq = new SiddhiQuery(ringBuffer);
    }




}
