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

package org.wso2.siddhi.debs2017.input.rabbitmq;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

/**
 * @deprecated
 */
public class ReadData {
    private static final String TASK_QUEUE_NAME = "test123";

    public static void main(String[] args) throws IOException, TimeoutException {
//        ConnectionFactory factory = new ConnectionFactory();
//        factory.setHost("localhost");
//        Connection connection = factory.newConnection();
//        Channel channel = connection.createChannel();
        String readData = "";

        //channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);


        BufferedReader reader = null;

        try{
            String dataInLine ;

            reader = new BufferedReader(new FileReader("frmattedData.txt"));
            String line = null;
            Scanner scanner = null;


            int count =0;
            while ((line = reader.readLine()) != null) {
                //System.out.println(line);
                scanner = new Scanner(line);
                // scanner.useDelimiter(".");

                while (scanner.hasNext() ) {
                    //System.out.println("----------------");
                    count++;
                    System.out.println(count);
                     dataInLine = scanner.next();
                    readData = readData.concat(dataInLine);
                }

            }
           // System.out.println(dataInLine);
            System.out.println("Data read" +readData);




        }catch (Exception e) {
            e.printStackTrace();
        }

        String[] streamedData = readData.split("----");

        for(int i=0; i<streamedData.length;i++){
          //  System.out.println("---");
            System.out.println(streamedData[i]);
        }

       /* for(int i =0; i < 3; i++) {

            for(int j =0; j<streamedData.length; j++)
            {


//                     count++;
//                     System.out.println(count);
                        String data1 = streamedData[j].replace("MoldingMachine_57", "MoldingMachine_" + i);
                        System.out.println(data1);
                        //data1 = "Hello World"+count;
                        *//* channel.basicPublish("", TASK_QUEUE_NAME,
                                 MessageProperties.PERSISTENT_TEXT_PLAIN,
                                 data1.getBytes());*//*




            }
        }
*/

       /* channel.close();
        connection.close();
*/
    }
}
