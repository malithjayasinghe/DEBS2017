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
public class SiddhiEventHandler implements EventHandler<EventWrapper> {

    private final long ID;
    private final long NUM;
    private final SiddhiQuery sq;

    @Override
    public void onEvent(EventWrapper wrapper, long sequence, boolean b) throws Exception {
        Object[] o = wrapper.getEvent().getData();
        String[] splitter = o[2].toString().split("_");
        long partition = Long.parseLong(splitter[2]);
        if (partition % NUM == ID) {
            sq.setSequence(sequence);
            sq.publish(wrapper.getEvent());
        }

    }

    /**
     * The constructor
     *
     * @param id         the ID
     * @param num        number
     * @param ringBuffer the ring buffer
     */
    public SiddhiEventHandler(long id, long num, RingBuffer<EventWrapper> ringBuffer) {
        this.ID = id;
        this.NUM = num;
        this.sq = new SiddhiQuery(ringBuffer);
    }
}
