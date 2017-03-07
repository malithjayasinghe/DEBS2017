package org.wso2.siddhi.debs2017.input;

import com.lmax.disruptor.RingBuffer;
import org.apache.log4j.Logger;
import org.wso2.siddhi.debs2017.processor.DebsEvent;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Scanner;

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
public class DebsDataPublisher {

    private String fileName;
    private static final Logger log = Logger.getLogger(DataPublisher.class);

    public static int count;
    public static int superCount;
    private final RingBuffer<DebsEvent> ringBuffer;

    /**
     * The constructor
     *
     * @param fileName the file name to read the data
     */
    public DebsDataPublisher(String fileName, RingBuffer<DebsEvent> ringBuffer)
    {
        this.ringBuffer = ringBuffer;

        this.fileName = fileName;



    }

    /**
     * Publishes data into Siddhi
     */
    public void publish() {

        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(fileName));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                Scanner scanner = new Scanner(line);
                scanner.useDelimiter(",");
                while (scanner.hasNext()) {

                    String machineName = scanner.next();
                    String timeStamp = scanner.next();
                    String sentTime = scanner.next();


                    String property = scanner.next();
                    double value = Double.parseDouble(scanner.next());


                    long sequence = ringBuffer.next();  // Grab the next sequence
                    try {
                        DebsEvent event = ringBuffer.get(sequence); // Get the entry in the Disruptor
                        event.setMachine(machineName);
                        event.settStamp(timeStamp);
                        event.setuTime(UnixConverter.getUnixTime(sentTime));
                        event.setDimension(property);
                        event.setValue(value);
                        event.setIj_time(System.currentTimeMillis());
                        event.setSentTime(sentTime);
                        // for the sequence

                    } finally {

                        ringBuffer.publish(sequence);


                    }
                    count++;

                }
            }

            superCount = count;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
