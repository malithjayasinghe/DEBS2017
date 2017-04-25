package org.wso2.siddhi.debs2017.query;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import org.hobbit.core.data.RabbitQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.debs2017.input.DebsBenchmarkSystem;
import org.wso2.siddhi.debs2017.input.metadata.DebsMetaData;
import org.wso2.siddhi.debs2017.input.metadata.RegexMetaData;
import org.wso2.siddhi.debs2017.input.rabbitmq.RabbitMQConsumer;
import org.wso2.siddhi.debs2017.input.sparql.*;
import org.wso2.siddhi.debs2017.output.AlertGenerator;
import org.wso2.siddhi.debs2017.processor.DebsAnomalyDetector;
import org.wso2.siddhi.debs2017.processor.SiddhiEventHandler;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

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

public class SingleNodeServer {

    private static final Logger logger = LoggerFactory.getLogger(SingleNodeServer.class);

    public static RingBuffer<RabbitMessage> buffer;


    public static long startime;
    public  static boolean isRegex = true;

    //ttl

    public static int maxClusterIterations = 50;
    public static int transitionsCount = 5;
    public static int windowSize = 10;


    public static void main(String[] args) {
        /**
         * 0 input queue
         * 1 output queue
         * 2 no. of rabbitmq executor threads
         * 3 isRegex
         * 4 no. of rehex handlers
         * 5 no. of siddhi handlers
         * 6  isTestcase
         * 7 machines
         */

        startime = System.currentTimeMillis();


      if (args.length == 9) {
          int rabbitMQExecutor = Integer.parseInt(args[2]);
          isRegex = Boolean.parseBoolean(args[3]);
          int regexHandlers = Integer.parseInt(args[4]);
          int siddhiHandlers = Integer.parseInt(args[5]);
          int ringbuffersize = Integer.parseInt(args[6]);
          boolean isTestcase = Boolean.parseBoolean(args[7]);
          int machines = Integer.parseInt(args[8]);

          //Disruptor
          Executor executor = Executors.newCachedThreadPool();
          Disruptor<RabbitMessage> disruptor = new Disruptor<>(RabbitMessage::new, ringbuffersize, executor);

          buffer = disruptor.getRingBuffer();
          //Creating RegexHandlers
          RegexHandler [] regexHandlerArr = new RegexHandler[regexHandlers];
          for(int i = 0; i<regexHandlers; i++){
              regexHandlerArr[i] = new RegexHandler(i, regexHandlers, buffer);
          }

          //Create SiddhiHandlers
          SiddhiEventHandler [] siddhiEventHandlerArr = new SiddhiEventHandler[siddhiHandlers];
          for(int i = 0; i<siddhiHandlers; i++){
              siddhiEventHandlerArr[i] = new SiddhiEventHandler(i, siddhiHandlers, buffer);
          }

          //Map to the disruptor


            if (args[0].equals("-hobbit")) {

                String metadata = args[1];
                RabbitQueue rmqPublisher = null;
                AlertGenerator alertGenerator = null;

                DebsAnomalyDetector debsAnormalyDetector = null;



                logger.debug("Running...");
                DebsBenchmarkSystem system = null;
                try {
                    system = new DebsBenchmarkSystem(metadata, rabbitMQExecutor);
                    system.init();

                    //disruptor
                    rmqPublisher = system.getOutputQueue();
                    alertGenerator = new AlertGenerator(rmqPublisher, false);
                    debsAnormalyDetector = new DebsAnomalyDetector(alertGenerator);


                    disruptor.handleEventsWith(regexHandlerArr);
                    disruptor.after(regexHandlerArr).handleEventsWith(siddhiEventHandlerArr);
                    disruptor.after(siddhiEventHandlerArr).handleEventsWith(debsAnormalyDetector);
                    disruptor.start();


                    system.run();
                } finally {
                    if (system != null) {
                        system.close();
                    }
                }
                logger.debug("Finished.");
            } else {

                String inputQueue = args[0];
                String outputQueue = args[1];
                RabbitQueue output = null;
                ConnectionFactory factory = new ConnectionFactory();
                factory.setHost("127.0.0.1");
                Channel channel = null;

                try {
                    channel = factory.newConnection().createChannel();
                    channel.basicQos(1);
                    channel.queueDeclare(outputQueue, false, false, true, null);
                } catch (Exception e) {
                    e.printStackTrace();
                }

                //check if regex
                if(isRegex){
                    if(isTestcase){
                        switch (machines){
                            case 1 : RegexMetaData.load("molding_machine_10M.metadata.nt"); break;
                            case 2 : RegexMetaData.load("molding_machine_10M.metadata.nt"); break;
                            case 3 : RegexMetaData.load("10molding_machine_5000dp.metadata.nt"); break;
                        }
                    } else  {
                        RegexMetaData.generate("molding_machine_10M.metadata.nt", machines);
                    }
                } else {
                    if(isTestcase){
                        switch (machines){
                            case 1 : DebsMetaData.load("molding_machine_10M.metadata.nt"); break;
                            case 2 : DebsMetaData.load("molding_machine_10M.metadata.nt"); break;
                            case 3 : DebsMetaData.load("10molding_machine_5000dp.metadata.nt"); break;
                        }
                    } else  {
                        DebsMetaData.generate("molding_machine_10M.metadata.nt", machines);
                    }
                }




                output = new RabbitQueue(channel, outputQueue);
                AlertGenerator alertGenerator = new AlertGenerator(output, false);
                DebsAnomalyDetector debsAnormalyDetector = new DebsAnomalyDetector(alertGenerator);



                disruptor.handleEventsWith(regexHandlerArr);
                disruptor.after(regexHandlerArr).handleEventsWith(siddhiEventHandlerArr);
                disruptor.after(siddhiEventHandlerArr).handleEventsWith(debsAnormalyDetector);
                disruptor.start();

                RabbitMQConsumer rmq = new RabbitMQConsumer();
                rmq.consume(inputQueue, rabbitMQExecutor);



            }

        }
    }
}
