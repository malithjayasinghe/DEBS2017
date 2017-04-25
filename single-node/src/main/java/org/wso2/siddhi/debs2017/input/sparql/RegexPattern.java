package org.wso2.siddhi.debs2017.input.sparql;

import com.lmax.disruptor.RingBuffer;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.debs2017.input.UnixConverter;
import org.wso2.siddhi.debs2017.input.metadata.DebsMetaData;
import org.wso2.siddhi.debs2017.query.SingleNodeServer;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by sachini on 4/20/17.
 */
public class RegexPattern {
    private static RingBuffer<RabbitMessage> ringBuffer;
    private final long timestamp;
    private byte [] data;
    private boolean findProp = false;
    private LinkedBlockingQueue<Event> queue;
    static Pattern patternTime = Pattern.compile("ationResultTime>.<http://project-hobbit.eu/resources/debs2017#(\\w*)>");
    static Pattern patternTimestamp = Pattern.compile("valueLiteral>.\"(.*)\"\\^");
    static Pattern patternMachine = Pattern.compile("WeidmullerMetadata#(.*)>");
    static  Pattern patternProperty = Pattern.compile("WeidmullerMetadata#(\\w*)>");
    static Pattern patternValue = Pattern.compile("debs2017#Value_.*>.<http://www.agtinternational.com/ontologies/IoTCore#valueLiteral>.\"(.*)\"\\^\\^<http://www.w3.org/2001/XMLSchema#");


    InputStream is = null;
    BufferedReader bfReader = null;

    public void process() {

        int count = 0;
        int propCount = 12;
        int valCount = 16;
        int nextOccurrence = 8;

        String propertyLine="";
        String time = "";
        String timeStamp = "";
        String machine = "";

        long uTime = 0l;
        try {
            is = new ByteArrayInputStream(data);
            bfReader = new BufferedReader(new InputStreamReader(is));
            String temp = null;
            while((temp = bfReader.readLine()) != null){
                count++;
                if(count==2){
                    Matcher matcher1 = patternTime.matcher(temp);
                    if (matcher1.find()) {

                        time = matcher1.group(1);

                    }

                } else  if(count==3) {
                    Matcher matcher3 = patternMachine.matcher(temp);
                    if (matcher3.find()) {

                        machine = matcher3.group(1);
                    }

                } else  if(count==8) {


                    Matcher matcher2 = patternTimestamp.matcher(temp);
                    if (matcher2.find()) {
                        timeStamp = matcher2.group(1);
                        uTime = UnixConverter.getUnixTime(timeStamp);
                    }
                } else if (count==propCount){

                    propertyLine = temp;

                    propCount+=nextOccurrence;
                }else if (count==valCount){

                    valCount+=nextOccurrence;

                    long sequence = SingleNodeServer.buffer.next();  // Grab the next sequence

                     RabbitMessage message = SingleNodeServer.buffer.get(sequence); // Get the entry in the Disruptor
                        message.setMachine(machine);
                        message.setTimestamp(time);
                        message.setTime(uTime);
                        message.setProperty(propertyLine);
                        message.setValue(temp);
                        message.setApplicationTime(this.timestamp);
                        SingleNodeServer.buffer.publish(sequence);


                }


            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try{
                if(is != null) is.close();
            } catch (Exception ex){

            }
        }

       // System.out.println("Time for line processor"+ "\t"+(System.currentTimeMillis() - start));
    }


    public static void publishTerminate(long timestamp){
        long sequence = SingleNodeServer.buffer.next();  // Grab the next sequence
        try {
            RabbitMessage wrapper = SingleNodeServer.buffer.get(sequence); // Get the entry in the Disruptor
            wrapper.setApplicationTime(timestamp);
            wrapper.setStateful(true);
            wrapper.setTerminated(true);
            wrapper.setEvent(new Event(-1l, new Object[]{}));

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