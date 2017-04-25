package org.wso2.siddhi.debs2017.input.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

import java.io.BufferedReader;
import java.io.FileReader;
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

/**
 * RabbitMQSamplePublisher
 */
public class RabbitMQSamplePublisher {
    private static String taskQueueName = "test123";


    /**
     * input queue name
     * no. of machines
     * isTest -  true/false
     */
    public static void main(String[] argv)
            throws java.io.IOException, TimeoutException {

        if (argv.length > 0) {
            taskQueueName = argv[0];
        }
        int machines = Integer.parseInt(argv[1]);

        boolean isTest = Boolean.parseBoolean(argv[2]);

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare(taskQueueName, true, false, false, null);
        BufferedReader reader = null;
        String data = "";
        String delimeter = "----";
        try {

            if (isTest) {
                switch (machines) {
                    case 1:
                        reader = new BufferedReader(new FileReader("frmattedData_63.nt"));
                        break;
                    case 2:
                        reader = new BufferedReader(new FileReader("formatted5000_1machine.txt"));
                        break;
                    case 3:
                        reader = new BufferedReader(new FileReader("formatted5000_10machines.txt"));
                        break;
                    default:
                        break;
                }

            } else {
                reader = new BufferedReader(new FileReader("frmattedData_63.nt"));

            }

            //molding_machine_100M.nt rdfSample.txt //Machine_59 //frmattedData.txt//frmattedData_63.nt
            // read file line by line
            String line;
            Scanner scanner;
            int count = 0;
            while ((line = reader.readLine()) != null) {
                scanner = new Scanner(line);
                while (scanner.hasNext()) {
                    String dataInLine = scanner.next();
                    if (dataInLine.contains(delimeter)) {

                        if (isTest) {
                            channel.basicPublish("", taskQueueName,
                                    MessageProperties.PERSISTENT_TEXT_PLAIN,
                                    data.getBytes());
                            count++;

                        } else {
                            for (int i = 0; i < machines; i++) {
                                count++;
                                String data1 = data.replace("Machine_59", "Machine_" + i).
                                        replace("_59_", "_" + i + "_");

                                channel.basicPublish("", taskQueueName,
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

            String terminationMessage = "~~Termination Message~~";
            channel.basicPublish("", taskQueueName,
                    MessageProperties.PERSISTENT_TEXT_PLAIN,
                    terminationMessage.getBytes());


        } catch (Exception e) {

        }

        channel.close();
        connection.close();
    }
}
