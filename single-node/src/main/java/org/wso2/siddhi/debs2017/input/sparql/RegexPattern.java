package org.wso2.siddhi.debs2017.input.sparql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.debs2017.input.UnixConverter;
import org.wso2.siddhi.debs2017.query.SingleNodeServer;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by sachini on 4/20/17.
 */
public class RegexPattern {
    private static final Logger logger = LoggerFactory.getLogger(RegexPattern.class);
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

                    long sequence = SingleNodeServer.buffer.next();  // Grab the next sequence

                    RabbitMessage message = SingleNodeServer.buffer.get(sequence); // Get the entry in the Disruptor
                    message.setMachine(machine);
                    message.setTimestamp(time);
                    message.setTime(uTime);
                    message.setProperty(propertyLine);
                    message.setValue(temp);
                    message.setApplicationTime(this.timestamp);
                    message.setLine(line);
                    SingleNodeServer.buffer.publish(sequence);
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


    public static void publishTerminate(long timestamp) {
        long sequence = SingleNodeServer.buffer.next();  // Grab the next sequence
        try {
            RabbitMessage wrapper = SingleNodeServer.buffer.get(sequence); // Get the entry in the Disruptor
            wrapper.setApplicationTime(timestamp);
            wrapper.setStateful(true);
            wrapper.setTerminated(true);
            wrapper.setEvent(new Event(-1L, new Object[]{}));

        } finally {

            SingleNodeServer.buffer.publish(sequence);


        }
    }

    public RegexPattern(byte[] data, long timestamp) {
        this.data = data;
        this.timestamp = timestamp;
        // this.ringBuffer = ringBuffer;


    }
}
