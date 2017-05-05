package org.wso2.siddhi.debs2017.input.sparql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.debs2017.input.UnixConverter;
import org.wso2.siddhi.debs2017.input.metadata.DebsMetaData;
import org.wso2.siddhi.debs2017.input.metadata.MetaDataItem;
import org.wso2.siddhi.debs2017.query.CentralDispatcher;

import java.io.*;
import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by sachini on 5/2/17.
 */
public class LineProcessor implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(LineProcessor.class);
    private final long timestamp;
    private byte[] data;
    static Pattern patternTime = Pattern.compile("ationResultTime>.<http://project-hobbit.eu/" +
            "resources/debs2017#(\\w*)>");
    static Pattern patternTimestamp = Pattern.compile("valueLiteral>.\"(.*)\"\\^");
    static Pattern patternMachine = Pattern.compile("WeidmullerMetadata#(.*)>");
    static Pattern patternProperty = Pattern.compile("WeidmullerMetadata#(\\w*)>");
    static Pattern patternValue = Pattern.compile("debs2017#Value_.*>.<http://www.agtinternational" +
            ".com/ontologies/IoTCore#valueLiteral>.\"(.*)\"\\^\\^<http://www.w3.or" +
            "g/2001/XMLSchema#");


    InputStream is = null;
    BufferedReader bfReader = null;
    private LinkedBlockingQueue<ObservationGroup> queue;

    public LineProcessor(byte[] data, long timestamp){
        this.data = data;
        this.timestamp = timestamp;
    }

    @Override
    public void run() {
        ObservationGroup ob;
        ArrayList<Event> arr = new ArrayList<>();
        this.queue = CentralDispatcher.arrayList.get(Integer.parseInt(Thread.currentThread().getName()));
        int count = 0;
        int propCount = 12;
        int valCount = 16;
        int nextOccurrence = 8;
        int line =0;
        String propertyLine = "";
        String time = "";
        String timeStamp = "";
        String machine = "";
        String property ="";
        String value = "";
        long uTime = 0L;
        try {
            is = new ByteArrayInputStream(data);
            bfReader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
            String temp = null;
            while ((temp = bfReader.readLine()) != null) {
                count++;
                if (count == 2) {
                    Matcher matcher1 = patternTime.matcher(temp);
                    if (matcher1.find()) {

                        time = matcher1.group(1);

                    }

                } else if (count == 3) {
                    Matcher matcher3 = patternMachine.matcher(temp);
                    if (matcher3.find()) {

                        machine = matcher3.group(1);
                    }

                } else if (count == 8) {


                    Matcher matcher2 = patternTimestamp.matcher(temp);
                    if (matcher2.find()) {
                        timeStamp = matcher2.group(1);
                        uTime = UnixConverter.getUnixTime(timeStamp);
                    }
                } else if (count == propCount) {

                    Matcher matchProp = patternProperty.matcher(temp);
                    if (matchProp.find()) {
                        property = matchProp.group(1);

                    }

                    propCount += nextOccurrence;
                } else if (count == valCount) {
                    valCount += nextOccurrence;
                    MetaDataItem metaDataItem = null;
                    metaDataItem = DebsMetaData.getMetaData().get(property);

                    if (metaDataItem != null) {
                        Matcher matchValue = patternValue.matcher(temp);
                        if (matchValue.find()) {
                            value = matchValue.group(1);

                        }
                        int centers = metaDataItem.getClusterCenters();
                        double probability = metaDataItem.getProbabilityThreshold();

                        Event event = new Event(this.timestamp, new Object[]{
                                machine,
                                time,
                                property,
                                uTime,
                                Double.parseDouble(value),
                                //Math.round(Double.parseDouble(value) * 10000.0) / 10000.0, //
                                centers,
                                probability,
                                0});

                        arr.add(event);

                    }



                }


            }
            ob = new ObservationGroup(this.timestamp, arr);
            try {
                this.queue.put(ob);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } catch (IOException e) {
            logger.debug(e.getMessage());
        } finally {
            try {
                if (is != null) {
                    is.close();
                }
            } catch (Exception ex) {
                logger.debug(ex.getMessage());
            }
        }


    }
}
