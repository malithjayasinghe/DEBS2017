package org.wso2.siddhi.debs2017.transport;

import com.lmax.disruptor.RingBuffer;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.debs2017.transport.processor.EventWrapper;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.tcp.transport.callback.StreamListener;

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
public class SiddhiListener implements StreamListener {
    // private static final Logger log = Logger.getLogger(org.wso2.siddhi.tcp.transport.callback.LogStreamListener.class);
    private StreamDefinition streamDefinition;
    private RingBuffer<EventWrapper> ringBuffer;

    public SiddhiListener(StreamDefinition streamDefinition, RingBuffer<EventWrapper> ring) {
        this.streamDefinition = streamDefinition;
        this.ringBuffer = ring;

    }

    @Override
    public StreamDefinition getStreamDefinition() {
        return streamDefinition;
    }

    @Override
    public void onEvent(Event event) {

    }

    @Override
    public void onEvents(Event[] events) {

        for (int i = 0; i < events.length; i++) {
            long sequence = this.ringBuffer.next();  // Grab the next sequence
            try {
                EventWrapper wrapper = this.ringBuffer.get(sequence); // Get the entry in the Disruptor
                wrapper.setEvent(events[i]);
            } finally {
                this.ringBuffer.publish(sequence);
            }
        }
    }
}
