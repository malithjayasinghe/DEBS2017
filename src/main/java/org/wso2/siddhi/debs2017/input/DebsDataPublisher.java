package org.wso2.siddhi.debs2017.input;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.debs2017.processor.DebsEventProducer;

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
    private DebsEventProducer prod;
    public static int count;
    public static int superCount;

    /**
     * The constructor
     *
     * @param fileName the file name to read the data
     */
    public DebsDataPublisher(String fileName, DebsEventProducer producer) {

        this.fileName = fileName;
        this.prod = producer;


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
                    long timeValue = UnixConverter.getUnixTime(sentTime);

                    String property = scanner.next();
                    double value = Double.parseDouble(scanner.next());

                    prod.onData(machineName, timeStamp, timeValue, property, value, System.currentTimeMillis(), sentTime);
                    count++;

                }
            }

            superCount = count;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
