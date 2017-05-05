package org.wso2.siddhi.debs2017.transport.processor;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.debs2017.input.metadata.DebsMetaData;
import org.wso2.siddhi.debs2017.input.metadata.MetaDataItem;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
public class RegexEventHandler implements EventHandler<EventWrapper> {

    private final int ID;
    private final int NUM;
    private final SiddhiQuery sq;
    private static Pattern patternProperty = Pattern.compile("WeidmullerMetadata#(\\w*)>");
    private static Pattern patternValue = Pattern.compile("debs2017#Value_.*>.<http://www.agtinternational" +
            ".com/ontologies/IoTCore#valueLiteral>.\"(.*)\"\\^\\^<http://www.w3.or" +
            "g/2001/XMLSchema#");
    private RingBuffer<EventWrapper> ring;

    @Override
    public void onEvent(EventWrapper wrapper, long sequence, boolean b) throws Exception {
       // System.out.println("siddhi :" +wrapper.getEvent());

        if (wrapper.getEvent().getTimestamp() == -1l) {
            System.out.println("regex : terminated");
            try {
                EventWrapper message = ring.get(sequence);
                //message.setEvent(wrapper.getEvent());
                message.setStateful(false);
            } finally {
                ring.publish(sequence);

            }
        } else {
            String property = "";
            String value = "";



            int partition = wrapper.getLine();


            if (partition % NUM == ID ) {
            //System.out.println("-"+partition+"----"+NUM+"-"+ID+"----"+wrapper.getEvent());
                //extract pattern
                Object[] o = wrapper.getEvent().getData();
                Matcher matcherProp = patternProperty.matcher(o[2].toString());
                if (matcherProp.find()) {
                    property = matcherProp.group(1);
                    MetaDataItem metaDataItem = DebsMetaData.getMetaData().get(property);


                    if (metaDataItem != null) {

                        Matcher matchValue = patternValue.matcher(o[4].toString());
                        if (matchValue.find()) {
                            value = matchValue.group(1);
                            int centers = metaDataItem.getClusterCenters();
                            double probability = metaDataItem.getProbabilityThreshold();

                            Event event = new Event(wrapper.getEvent().getTimestamp(), new Object[]{
                                    o[0].toString(),
                                    o[1].toString(),
                                    property,
                                    Long.parseLong(o[3].toString()),
                                    Double.parseDouble(value),
                                    centers,
                                    probability,
                                    Integer.parseInt(o[5].toString())
                            });
                            this.sq.setSequence(sequence);
                            this.sq.publish(event);

                        }


                    } else {
                        //publish un stateful events back to the ring
                        try {
                            EventWrapper message = this.ring.get(sequence);
                            message.setStateful(false);
                        } finally {
                            this.ring.publish(sequence);

                        }
                    }

                }


            }
        }

    }

    /**
     * The constructor
     *
     * @param id         the ID
     * @param num        number
     * @param ringBuffer the ring buffer
     */
    public RegexEventHandler(int id, int num, RingBuffer<EventWrapper> ringBuffer) {
        this.ID = id;
        this.NUM = num;
        this.sq = new SiddhiQuery(ringBuffer);
        this.ring = ringBuffer;
    }
}
