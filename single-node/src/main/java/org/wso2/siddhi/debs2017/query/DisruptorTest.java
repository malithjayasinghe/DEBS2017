package org.wso2.siddhi.debs2017.query;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
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
public class DisruptorTest {

    public static void main(String[] args) {

        BufferedReader reader = null;


        try {

            reader = new BufferedReader(new FileReader("disruptor_67.csv"));
            int index = 0;
            // read file line by line
            String line = null;
            Scanner scanner = null;
            int count = 0;
            String delimeter = ",";
            ArrayList<String> arr = new ArrayList<>(55);
            while ((line = reader.readLine()) != null) {

                scanner = new Scanner(line);
                scanner.useDelimiter(delimeter);

                String pub = "";
                while (scanner.hasNext() ) {

                    String data = scanner.next();
                    if(index==0){
                        pub += ""+data;
                    } else if(index==1){
                        pub += ","+data;
                    } else if(index==2){
                        pub += ","+data;
                    } else if(index==3){
                        pub += ","+data;
                    } else if(index==4){
                        pub += ","+data;
                    } else if(index==5){
                        pub += ","+data;
                    } else if(index==6){

                        pub += ","+data;

                        arr.add(pub);
                        if(arr.size()==55) {
                            for (int i = 0; i < 2; i++) {
                            for (int j = 0; j < arr.size(); j++) {

                                    String pub1 = arr.get(j).replace("Machine_59", "Machine_" + i).replace("_59_", "_" + i + "_");
                                System.out.println(pub1);
                                }

                            }
                            arr = new ArrayList<>(55);
                        }
                    }

                    index++;


                    count++;
                }
                index = 0;
                //System.out.println("------------");
                count = 0;
            }


            //inputHandler.send(new Object[]{windspeed});


            //close reader

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
