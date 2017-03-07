package org.wso2.siddhi.debs2017.processor;


import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.timestamp.SystemCurrentTimeMillisTimestampGenerator;
import org.wso2.siddhi.debs2017.input.DataPublisher;
import org.wso2.siddhi.debs2017.input.DebsDataPublisher;

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
public class DebsEventHandler implements EventHandler<DebsEvent> {

    private final long ordinal;
    private final long num;
    private final SiddhiQuery sq;


    @Override
    public void onEvent(DebsEvent debsEvent, long sequence, boolean b) throws Exception {

        long machine = Long.parseLong(debsEvent.getMachine().substring(15));
        long dimension = Long.parseLong(debsEvent.getDimension().substring(1));
        if (dimension % num == ordinal) {

            //setting the event to be altered
            sq.setEvent(debsEvent);
            //setting the buffer sequence
            sq.setSequence(sequence);
            sq.publish(new Object[]{debsEvent.getMachine(), debsEvent.gettStamp(), debsEvent.getuTime(),
                    debsEvent.getDimension(), debsEvent.getValue(), debsEvent.getIj_time()});
        }

    }


    public DebsEventHandler(long id, long numOfCon, RingBuffer<DebsEvent> buffer){
        this.num = numOfCon;
        this.ordinal = id;
        this.sq = new SiddhiQuery(buffer);

    }


}

