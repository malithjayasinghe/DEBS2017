package org.wso2.siddhi.debs2017.input;


import io.netty.bootstrap.Bootstrap;
import org.apache.jena.base.Sys;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.debs2017.processor.DebsEvent;
import org.wso2.siddhi.debs2017.transport.SiddhiClient;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
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
public class SiddhiDataPublisher {

    private String fileName;
    private static  BufferedReader bufferedReader;
    String line;
    Scanner scanner;
    private SiddhiClient siddhiClient;
    public  static boolean IS_END = false;


    /**
     * The constructor
     *
     * @param fileName the file name to read the data
     */
    public SiddhiDataPublisher(String fileName, SiddhiClient siddhiClient) {


        this.fileName = fileName;
        this.siddhiClient = siddhiClient;
        try {
            this.bufferedReader = new BufferedReader(new FileReader(fileName));
        } catch (Exception e) {

        }


    }

    public SiddhiDataPublisher(String fileName) {

        this.fileName = fileName;
        try {
            this.bufferedReader = new BufferedReader(new FileReader(fileName));
        } catch (Exception e) {

        }


    }


    public Event publishData(){

        try {
            while ((line = bufferedReader.readLine()) != null) {
                scanner = new Scanner(line);
                scanner.useDelimiter(",");
                while (scanner.hasNext()) {

                    String machineName = scanner.next();
                    String timeStamp = scanner.next();
                    String sentTime = scanner.next();


                    String property = scanner.next();
                    double value = Double.parseDouble(scanner.next());


                   return new Event(System.currentTimeMillis(), new Object[]{machineName, timeStamp, property, sentTime, value});





                }

            }
            IS_END=true;

        } catch (IOException e) {
            e.printStackTrace();
        }


        return new Event(System.currentTimeMillis(), new Object[]{-1, -1, -1, -1});
    }



    }


