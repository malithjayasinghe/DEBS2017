package org.wso2.siddhi.debs2017.input;


import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.MessageProperties;
import org.hobbit.core.Commands;
import org.hobbit.core.Constants;
import org.hobbit.core.components.AbstractCommandReceivingComponent;
import org.hobbit.core.data.RabbitQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.debs2017.input.metadata.DebsMetaData;
import org.wso2.siddhi.debs2017.input.metadata.RegexMetaData;
import org.wso2.siddhi.debs2017.input.sparql.RegexPattern;
import org.wso2.siddhi.debs2017.query.SingleNodeServer;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
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

public class DebsBenchmarkSystem extends AbstractCommandReceivingComponent {
    private static final Logger logger = LoggerFactory.getLogger(DebsBenchmarkSystem.class);
    private static final String TERMINATION_MESSAGE = "~~Termination Message~~";
    private static final Charset CHARSET = Charset.forName("UTF-8");

    private final CountDownLatch startExecutionBarrier = new CountDownLatch(1);
    private final CountDownLatch terminationMessageBarrier = new CountDownLatch(1);

    private RabbitQueue inputQueue;
    private RabbitQueue outputQueue;

    private static ExecutorService RMQ_EXECUTOR;

    public DebsBenchmarkSystem(String metadataFile, int rabbitMQExec){
        RMQ_EXECUTOR = Executors.newFixedThreadPool(rabbitMQExec);
        if(SingleNodeServer.isRegex){
            RegexMetaData.load(metadataFile);
        } else {
            DebsMetaData.load(metadataFile);
        }



    }

    public RabbitQueue getOutputQueue(){
        return outputQueue;
    }

    @Override
    public void init()  {

        try {
            logger.debug("Initializing...");
            super.init();
            String hobbitSessionId = getHobbitSessionId();
            if (hobbitSessionId.equals(Constants.HOBBIT_SESSION_ID_FOR_BROADCASTS) ||
                    hobbitSessionId.equals(Constants.HOBBIT_SESSION_ID_FOR_PLATFORM_COMPONENTS)) {
                throw new IllegalStateException("Wrong hobbit session id. It must not be equal to HOBBIT_SESSION_ID_FOR_BROADCASTS or HOBBIT_SESSION_ID_FOR_PLATFORM_COMPONENTS");
            }
            initCommunications();
            logger.debug("Initialized");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void initCommunications() throws Exception {
        outputQueue = createQueueWithName(getOutputQueueName(), null);
        inputQueue = createQueueWithName(getInputQueueName() , RMQ_EXECUTOR);
        registerConsumerFor(inputQueue);
    }



    private RabbitQueue createQueueWithName(String name, ExecutorService executorService) throws Exception {
        Channel channel = createConnection(executorService).createChannel();
        channel.basicQos(getPrefetchCount());
        channel.queueDeclare(name, false, false, true, null);
        return new RabbitQueue(channel, name);
    }

    private Connection createConnection(ExecutorService executorService) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(getHost());
        if(executorService.equals(null)){
            return factory.newConnection();
        }
        return factory.newConnection(executorService);
    }

    private void registerConsumerFor(RabbitQueue queue) throws IOException {
        Channel channel = queue.getChannel();
        channel.basicConsume(queue.getName(), false, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag,
                                       Envelope envelope,
                                       AMQP.BasicProperties properties,
                                       byte[] body) throws IOException {
                getChannel().basicAck(envelope.getDeliveryTag(), false);
                DebsBenchmarkSystem.this.handleDelivery(body);
            }
        });
    }

    public String getHost() {
        return System.getenv().get(Constants.RABBIT_MQ_HOST_NAME_KEY);
    }

    private int getPrefetchCount() {
        return 1;
    }

    private String getInputQueueName() {
        return toPlatformQueueName(Constants.DATA_GEN_2_SYSTEM_QUEUE_NAME);
    }

    private static String toPlatformQueueName(String queueName) {
        return queueName + "." + System.getenv().get(Constants.HOBBIT_SESSION_ID_KEY);
    }

    public String getOutputQueueName() {
        return toPlatformQueueName(Constants.SYSTEM_2_EVAL_STORAGE_QUEUE_NAME);
    }

    @Override
    public void run()  {

        try {

            logger.debug("Sending SYSTEM_READY_SIGNAL...");
            sendToCmdQueue(Commands.SYSTEM_READY_SIGNAL);   // Notifies PlatformController that it is ready to start
            logger.debug("Waiting for TASK_GENERATION_FINISHED...");
            startExecutionBarrier.await();
            logger.debug("Starting system execution...");
            execute();
            logger.debug("Finished");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public void receiveCommand(byte command, byte[] data) {
        if (command == Commands.TASK_GENERATION_FINISHED) {
            startExecutionBarrier.countDown();
        }
    }

    /**
     * This is where system execution starts when it receives {@code Commands.TASK_GENERATION_FINISHED}
     * from the PlatformController. Since all the processing done upon receiving a message in {@link #handleDelivery(byte[])}
     * this method is just blocked.
     */
    private void execute() {
        try {
            logger.debug("Waiting for termination message...");
            terminationMessageBarrier.await();
            logger.debug("Sending termination message...");
            sendTerminationMessage();
        } catch (Exception e) {
            logger.error("Exception", e);
        }
        logger.debug("Execution finished.");
    }

    private void sendTerminationMessage() {

        try {

            logger.debug("Sending termination message to: {} sender: {}", outputQueue.getName(), this);
            send(TERMINATION_MESSAGE);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void send(String string) {
        try {
            send(string.getBytes(CHARSET));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void send(byte[] bytes)  {
        Channel channel = outputQueue.getChannel();
        try {
            channel.basicPublish("", outputQueue.getName(), MessageProperties.PERSISTENT_BASIC, bytes);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void handleDelivery(byte[] bytes) {
        try {
            //String message = new String(bytes, CHARSET);
            if(bytes.length<30){
                    RegexPattern.publishTerminate(System.nanoTime());

            } else {

                RegexPattern regexPattern = new RegexPattern(bytes,System.nanoTime());
                regexPattern.process();


            }
        } catch (Exception e) {
            logger.error("Exception", e);
        }
    }


    @Override
    public void close()  {

        try {
            super.close();
            Channel channel = inputQueue.getChannel();
            Connection connection = channel.getConnection();
            channel.close();
            connection.close();
            channel = outputQueue.getChannel();
            connection = channel.getConnection();
            channel.close();
            connection.close();
        } catch (Exception e) {
            logger.debug("Exception", e);
        }
    }
}

