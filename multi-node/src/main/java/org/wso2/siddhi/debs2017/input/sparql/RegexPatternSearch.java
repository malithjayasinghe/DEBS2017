package org.wso2.siddhi.debs2017.input.sparql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.debs2017.input.UnixConverter;
import org.wso2.siddhi.debs2017.query.CentralDispatcher;
import org.wso2.siddhi.debs2017.transport.utils.TcpNettyClient;

import java.io.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by sachini on 5/4/17.
 */
public class RegexPatternSearch {
    private static final Logger logger = LoggerFactory.getLogger(RegexPatternSearch.class);
    private final long timestamp;
    private byte[] data;
    static Pattern patternTime = Pattern.compile("ationResultTime>.<http://project-hobbit.eu/" +
            "resources/debs2017#(\\w*)>");
    static Pattern patternTimestamp = Pattern.compile("valueLiteral>.\"(.*)\"\\^");
    static Pattern patternMachine = Pattern.compile("WeidmullerMetadata#(.*)>");


    InputStream is = null;
    BufferedReader bfReader = null;

    public void process() {

        int count = 0;
        int propCount = 12;
        int valCount = 16;
        int nextOccurrence = 8;
        int line =0;
        String propertyLine = "";
        String time = "";
        String timeStamp = "";
        String machine = "";

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

                    propertyLine = temp;

                    propCount += nextOccurrence;
                } else if (count == valCount) {

                    valCount += nextOccurrence;
/*
                    long sequence = CentralDispatcher.buffer.next();  // Grab the next sequence

                    RdfMessage message = CentralDispatcher.buffer.get(sequence); // Get the entry in the Disruptor
                    message.setMachine(machine);
                    message.setTimestamp(time);
                    message.setTime(uTime);
                    message.setProperty(propertyLine);
                    message.setValue(temp);
                    message.setApplicationTime(this.timestamp);
                    message.setLine(line);
                    CentralDispatcher.buffer.publish(sequence);*/
                    int node = Integer.parseInt(machine.split("_")[1]);
                    Event event = new Event(this.timestamp, new Object[]{machine, time, propertyLine, uTime,
                                            temp, node,line});
                    if(node % 3 == 0) {
                        EventDispatcher.siddhiClient0.send("input", new Event[]{event});
                    }else if(node % 3 == 1){
                        EventDispatcher.siddhiClient1.send("input", new Event[]{event});
                    }else {
                        EventDispatcher.siddhiClient2.send("input", new Event[]{event});
                    }

                    line = line +1;


                }


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


    public static void publishTerminate() {
       /* long sequence = CentralDispatcher.buffer.next();  // Grab the next sequence
        try {
            RdfMessage wrapper = CentralDispatcher.buffer.get(sequence); // Get the entry in the Disruptor
            wrapper.setApplicationTime(timestamp);
            wrapper.setStateful(true);
            wrapper.setTerminated(true);
            wrapper.setEvent(new Event(-1L, new Object[]{}));
            System.out.println("Termination published");


        } finally {

            CentralDispatcher.buffer.publish(sequence);


        }*/
        Event e1 = new Event(-1l, new Object[]{"machine", "time", "dimension", -1L, "value", 0, 0});
        Event e2 = new Event(-1l, new Object[]{"machine", "time", "dimension", -1L, "value", 1, 1});
        Event e3 = new Event(-1l, new Object[]{"machine", "time", "dimension", -1L, "value", 2, 2});
        EventDispatcher.siddhiClient0.send("input",new Event[]{e1});
        EventDispatcher.siddhiClient1.send("input",new Event[]{e2});
        EventDispatcher.siddhiClient2.send("input",new Event[]{e3});

    }

    public RegexPatternSearch(byte[] data, long timestamp) {
        this.data = data;
        this.timestamp = timestamp;
        // this.ringBuffer = ringBuffer;


    }
}
