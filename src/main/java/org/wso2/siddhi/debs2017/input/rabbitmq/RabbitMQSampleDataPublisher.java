package org.wso2.siddhi.debs2017.input.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

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
public class RabbitMQSampleDataPublisher {

    private static final String TASK_QUEUE_NAME = "test123";

    public static void main(String[] argv)
            throws java.io.IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);
        String data = "";
        try {
            BufferedReader reader = new BufferedReader(new FileReader("frmattedData_63.nt"));//molding_machine_100M.nt rdfSample.txt //Machine_59 //frmattedData.txt
            // read file line by line
            String line ;
            Scanner scanner ;
            int count = 0;
            while ((line = reader.readLine()) != null) {
                scanner = new Scanner(line);
                while (scanner.hasNext()) {
                    String dataInLine = scanner.next();
                    if (dataInLine.contains("----")) {
                        if (data.length() > 100) {
                            for (int i = 0; i < 5; i++) {
                                count++;
                                System.out.println(count);
                                String data1 = data.replace("Machine_59", "Machine_" + i).replace("_59_", "_" + i + "_");
                                channel.basicPublish("", TASK_QUEUE_NAME,
                                        MessageProperties.PERSISTENT_TEXT_PLAIN,
                                        data1.getBytes());
                            }
                        }
                        data = "";
                    } else {
                        data += " " + dataInLine;
                        if (dataInLine.contains(".") && !dataInLine.contains("<")) {
                            data += "\n";
                        }
                    }
                }
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        channel.close();
        connection.close();
    }

}
