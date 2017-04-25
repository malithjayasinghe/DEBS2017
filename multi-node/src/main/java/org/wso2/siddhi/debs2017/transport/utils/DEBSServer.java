package org.wso2.siddhi.debs2017.transport.utils;

/**
 * DEBS Server
 */
public class DEBSServer {

    public enum SERVERTYPE {
        RABBITMQ_PUBLISHER,
        SIDDHI_SERVER,
        CENTRAL_DISPATCHER,
        OUTPUT_SERVER
    }

    /**
     * The entry point of the DEBS server
     *
     * @param args
     */
    public static void main(String[] args) {

        int serverType = Integer.parseInt(args[0]);

        if (serverType == SERVERTYPE.OUTPUT_SERVER.ordinal()) {
            //CentralDispatcher.start(args);
        }

        if (serverType == SERVERTYPE.CENTRAL_DISPATCHER.ordinal()) {
            //OutputServer.start(args);
        }

        if (serverType == SERVERTYPE.SIDDHI_SERVER.ordinal()) {
            // SiddhiServer.start(args);
        }


        if (serverType == SERVERTYPE.RABBITMQ_PUBLISHER.ordinal()) {
            //  RabbitMQSampleDataPublisher.start(args);
        }


    }
}
