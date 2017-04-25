package org.wso2.siddhi.debs2017.transport;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.hobbit.core.Commands;
import org.hobbit.core.Constants;
import org.hobbit.core.components.AbstractCommandReceivingComponent;
import org.hobbit.core.data.RabbitQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
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
public class DebsBenchmarkOutput extends AbstractCommandReceivingComponent {
    private static final Logger logger = LoggerFactory.getLogger(DebsBenchmarkOutput.class);
    private static final String TERMINATION_MESSAGE = "~~Termination Message~~";
    private static final Charset CHARSET = Charset.forName("UTF-8");

    private final CountDownLatch startExecutionBarrier = new CountDownLatch(1);
    private final CountDownLatch terminationMessageBarrier = new CountDownLatch(1);

    private RabbitQueue outputQueue;


    private static ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("%d").build();


    public DebsBenchmarkOutput() {

    }

    public RabbitQueue getOutputQueue() {
        return outputQueue;
    }

    @Override
    public void init() {

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
        if (executorService.equals(null)) {
            return factory.newConnection();
        }
        return factory.newConnection(executorService);
    }


    public String getHost() {
        return System.getenv().get(Constants.RABBIT_MQ_HOST_NAME_KEY);
    }

    private int getPrefetchCount() {
        return 1;
    }


    private static String toPlatformQueueName(String queueName) {
        return queueName + "." + System.getenv().get(Constants.HOBBIT_SESSION_ID_KEY);
    }

    public String getOutputQueueName() {
        return toPlatformQueueName(Constants.SYSTEM_2_EVAL_STORAGE_QUEUE_NAME);
    }

    @Override
    public void run() {

        try {

            logger.debug("Sending SYSTEM_READY_SIGNAL...");
            sendToCmdQueue(Commands.SYSTEM_READY_SIGNAL);   // Notifies PlatformController that it is ready to start
            logger.debug("Waiting for TASK_GENERATION_FINISHED...");
            startExecutionBarrier.await();
            logger.debug("Starting system execution...");
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


    @Override
    public void close() {

        try {
            super.close();
            Channel channel = outputQueue.getChannel();
            connection = channel.getConnection();
            channel.close();
            connection.close();
        } catch (Exception e) {
            logger.debug("Exception", e);
        }
    }
}
