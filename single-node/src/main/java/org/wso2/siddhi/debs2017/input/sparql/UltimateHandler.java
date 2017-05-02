package org.wso2.siddhi.debs2017.input.sparql;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.debs2017.input.metadata.DebsMetaData;
import org.wso2.siddhi.debs2017.input.metadata.MetaDataItem;
import org.wso2.siddhi.debs2017.input.metadata.RegexMetaData;
import org.wso2.siddhi.debs2017.processor.SiddhiQuery;
import org.wso2.siddhi.debs2017.query.SingleNodeServer;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by sachini on 4/28/17.
 */
public class UltimateHandler implements EventHandler<RabbitMessage> {
    private final int id;
    private final int num;
    private final RingBuffer<RabbitMessage> ringBuffer;
    static Pattern patternProperty = Pattern.compile("WeidmullerMetadata#(\\w*)>");
    static Pattern patternValue = Pattern.compile("debs2017#Value_.*>.<http://www.agtinternational" +
            ".com/ontologies/IoTCore#valueLiteral>.\"(.*)\"\\^\\^<http://www.w3.or" +
            "g/2001/XMLSchema#");
    String property = "";
    String value = "";
    private final SiddhiQuery sq;

    public UltimateHandler(int id, int handlers, RingBuffer ringBuffer) {
        this.id = id;
        num = handlers;
        this.ringBuffer = ringBuffer;
        this.sq = new SiddhiQuery(ringBuffer);
    }


    @Override
    public void onEvent(RabbitMessage message, long sequence, boolean b) throws Exception {

        if (!message.isTerminated()) {

            if (message.getLine() % num == id) {
                Matcher matcherProp = patternProperty.matcher(message.getProperty());
                if (matcherProp.find()) {
                    property = matcherProp.group(1);

                }

                MetaDataItem metaDataItem = null;
                if (SingleNodeServer.isRegex) {
                    metaDataItem = RegexMetaData.getMetaData().get(property);
                } else {
                    metaDataItem = DebsMetaData.getMetaData().get(property);
                }


                if (metaDataItem != null) {
                    Matcher matchValue = patternValue.matcher(message.getValue());
                    if (matchValue.find()) {
                        value = matchValue.group(1);

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


                    //setting the buffer sequence
                    sq.setSequence(sequence);

                    //publish event to sidhdhi
                    sq.publish(event);
                } else {
                    try {
                        message = ringBuffer.get(sequence);
                        message.setStateful(false);
                    } finally {
                        ringBuffer.publish(sequence);

                    }

                }
            }
        }

    }
}
