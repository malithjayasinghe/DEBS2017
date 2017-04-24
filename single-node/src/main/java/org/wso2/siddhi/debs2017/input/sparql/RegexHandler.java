package org.wso2.siddhi.debs2017.input.sparql;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.debs2017.input.metadata.DebsMetaData;
import org.wso2.siddhi.debs2017.input.metadata.MetaDataItem;
import org.wso2.siddhi.debs2017.processor.EventWrapper;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by sachini on 4/20/17.
 */
public class RegexHandler implements EventHandler<RabbitMessage> {
    private final int ID;
    private final int NUM;
    private final RingBuffer<RabbitMessage> ringBuffer;
    static Pattern patternProperty = Pattern.compile("WeidmullerMetadata#(\\w*)>");
    static Pattern patternValue = Pattern.compile("debs2017#Value_.*>.<http://www.agtinternational.com/ontologies/IoTCore#valueLiteral>.\"(.*)\"\\^\\^<http://www.w3.org/2001/XMLSchema#");
    String property = "";
    String value =  "";

    public RegexHandler(int ID, int handlers, RingBuffer ringBuffer){
      this.ID = ID;
      NUM = handlers;
      this.ringBuffer = ringBuffer;
    }


    @Override
    public void onEvent (RabbitMessage message , long sequence, boolean b) throws Exception {
        //System.out.println("On event regexhandler");
        if (!message.isTerminated()) {

            if (message.getApplicationTime() % NUM == ID) {
                Matcher matcherProp = patternProperty.matcher(message.getProperty());
                if (matcherProp.find()) {
                    property = matcherProp.group(1);
                    // System.out.println(property + "Property");
                }
                String stateful = property;
                MetaDataItem metaDataItem = DebsMetaData.getMetaData().get(stateful);
                if (metaDataItem !=null) {
                    Matcher matchValue = patternValue.matcher(message.getValue());
                    if (matchValue.find()) {
                        value = matchValue.group(1);
                        //System.out.println(value + "Value");
                    }
                    int centers = metaDataItem.getClusterCenters();
                    double probability = metaDataItem.getProbabilityThreshold();

                    Event event = new Event(message.getApplicationTime(), new Object[]{
                            message.getMachine(),
                            message.getTimestamp(),
                            property,
                            message.getTime(),
                            Double.parseDouble(value),
                            //Math.round(Double.parseDouble(value) * 10000.0) / 10000.0, //
                            centers,
                            probability});
                    //System.out.println(message.getMachine() + "\tt" + message.getTimestamp() + "\t"+ message.getTime() +"\t"
                    //  + property+"\t"+value);
                    try {
                        message = ringBuffer.get(sequence);

                        message.setEvent(event);
                        message.setStateful(true);

                    } finally {
                        ringBuffer.publish(sequence);
                        // System.out.println(message.getEvent()+ "Regex handler");

                    }
                } else {
                    try {
                        message = ringBuffer.get(sequence);
                        message.setStateful(false);
                    } finally {
                        ringBuffer.publish(sequence);
                        // System.out.println(message.isStateful()+ "Not stateful published");

                    }

                }
            }
        }

    }
    }


