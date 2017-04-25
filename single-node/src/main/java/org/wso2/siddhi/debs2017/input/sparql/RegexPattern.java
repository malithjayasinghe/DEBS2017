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

        //long start =  System.currentTimeMillis();
//        this.queue = SingleNodeServer.arraylist.get(Integer.parseInt(Thread.currentThread().getName()));
        int count = 0;
        int propCount = 12;
        int valCount = 16;
        int nextOccurrence = 8;

        String propertyLine="";
        String time = "";
        String timeStamp = "";
        String machine = "";
        String property = "";
        String value =  "";
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
                        //  System.out.println("----------------||-time");
                        time = matcher1.group(1);

                    }

                } else  if(count==3) {
                    Matcher matcher3 = patternMachine.matcher(temp);
                    if (matcher3.find()) {
                        //  System.out.println("-----------------machine");
                        machine = matcher3.group(1);
                    }

                } else  if(count==8) {

                    //System.out.println(count+"\t"+temp);
                    Matcher matcher2 = patternTimestamp.matcher(temp);
                    if (matcher2.find()) {
                        timeStamp = matcher2.group(1);
                        // System.out.println(timeStamp + "\t"+timeStamp.length());
                        uTime = UnixConverter.getUnixTime(timeStamp);
                    }
                } else if (count==propCount){
                   // findProp = true;
                    propertyLine = temp;
                    // System.out.println(temp);
                    propCount+=nextOccurrence;
                }else if (count==valCount){
                    // System.out.println(temp);
                    valCount+=nextOccurrence;
                   // System.out.println("-----------------"+SingleNodeServer.Buffer.getBufferSize());
                    long sequence = SingleNodeServer.Buffer.next();  // Grab the next sequence
                    try {
                     RabbitMessage    message = SingleNodeServer.Buffer.get(sequence); // Get the entry in the Disruptor
                        message.setMachine(machine);
                        message.setTimestamp(time);
                        message.setTime(uTime);
                        message.setProperty(propertyLine);
                        message.setValue(temp);
                       // message.setLine(count);
                        message.setApplicationTime(this.timestamp);
                      //  System.out.println(machine + "\t" + timeStamp + "\t"+ uTime + "\t" + count + "\t" + propertyLine + "\t" + temp);
                    } finally {

                        SingleNodeServer.Buffer.publish(sequence);

                       // System.out.println("Event published"+ "----------");
                    }
                }
                //  System.out.println(count+"\t"+temp);

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
        long sequence = SingleNodeServer.Buffer.next();  // Grab the next sequence
        try {
            RabbitMessage wrapper = SingleNodeServer.Buffer.get(sequence); // Get the entry in the Disruptor
            wrapper.setApplicationTime(timestamp);
            wrapper.setStateful(true);
            wrapper.setTerminated(true);
            wrapper.setEvent(new Event(-1l, new Object[]{}));
            //System.out.println(wrapper.getEvent());
        } finally {

            SingleNodeServer.Buffer.publish(sequence);
//            System.out.println(sequence + "Termination sequence");
//            System.out.println("Termination publised to disruptor");

        }
    }

    public RegexPattern(byte[] data, long timestamp) {
        this.data = data;
        this.timestamp = timestamp;
       // this.ringBuffer = ringBuffer;



    }
}