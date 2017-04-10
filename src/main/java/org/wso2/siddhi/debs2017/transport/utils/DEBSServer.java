package org.wso2.siddhi.debs2017.transport.utils;

import org.wso2.siddhi.debs2017.query.CentralDispatcher;

/**
 *
 * DEBS Server
 *
 */
public class DEBSServer {

    public enum SERVERTYPE
    {
        RABBITMQ_PUBLISHER,
        SIDDHI_SERVER,
        CENTRAL_DISPATCHER,
        OUTPUT_SERVER
    }
    public static void main(String[] args) {

        int serverType = Integer.parseInt(args[0]);

        if(serverType == SERVERTYPE.OUTPUT_SERVER.ordinal())
        {
            CentralDispatcher.start(args);
        }

        if(serverType == SERVERTYPE.CENTRAL_DISPATCHER.ordinal())
        {
            CentralDispatcher.start(args);
        }

        if(serverType == SERVERTYPE.SIDDHI_SERVER.ordinal())
        {

        }


        if(serverType == SERVERTYPE.RABBITMQ_PUBLISHER.ordinal())
        {

        }


    }
}
