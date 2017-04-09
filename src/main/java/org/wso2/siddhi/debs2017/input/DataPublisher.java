/*
 *
 *  Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * /
 *
 */

package org.wso2.siddhi.debs2017.input;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.stream.input.InputHandler;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Scanner;

/**
 * @deprecated
 */
public class DataPublisher {

    private String fileName;
    private static final Logger log = Logger.getLogger(DataPublisher.class);
    private InputHandler inputHandler;
    public  static  int count;
   public static int supercount;
   public static long startime;
   boolean start = false;

    /**
     * The constructor
     *
     * @param fileName     the file name to read the data
     * @param inputHandler the inputhandler of the execution plan
     */
    public DataPublisher(String fileName, InputHandler inputHandler) {

        this.fileName = fileName;
        this.inputHandler = inputHandler;

    }

    /**
     * Publishes data into Siddhi
     */
    public void publish() {

        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(fileName));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                if(start == false){
                    startime = System.currentTimeMillis();
                    start = true;
                }
                Scanner scanner = new Scanner(line);
                scanner.useDelimiter(",");
                while (scanner.hasNext()) {

                    String machineName = scanner.next();
                    String timeStamp = scanner.next();
                    String time = scanner.next();
                    long timeValue = UnixConverter.getUnixTime(time);

                    String property = scanner.next();
                    double value = Double.parseDouble(scanner.next());
                    long addedTime = System.currentTimeMillis();
                    try {

                        inputHandler.send(new Object[]{machineName, timeStamp, timeValue, property, addedTime,value});
                        //machine string, tstamp string, uTime long, dimension string, addedTime long, " +
                        //"value double
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    count++;
                }
            }
            supercount = count;
        } catch (Exception e) {
            e.printStackTrace();
            log.info(e);
        }
    }

}
