package org.wso2.siddhi.debs2017.transport.processor;

import com.lmax.disruptor.EventHandler;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.debs2017.Output.AlertGenerator;
import org.wso2.siddhi.debs2017.input.DebsDataPublisher;
import org.wso2.siddhi.debs2017.processor.DebsEvent;
import org.wso2.siddhi.debs2017.query.DistributedQuery;
import org.wso2.siddhi.tcp.transport.TcpNettyClient;

import java.util.ArrayList;

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
public class DebsAnormalyDetector implements EventHandler<EventWrapper> {

    private TcpNettyClient siddhiClient;


    @Override
    public void onEvent(EventWrapper wrapper, long l, boolean b) throws Exception {

        //System.out.println(wrapper.getEvent());
        //Object [] o = wrapper.getEvent().getData();

        //double prbability = Double.parseDouble(o[5].toString());

        send(wrapper.getEvent());

    }

    private synchronized void send(Event event) {

        Event [] events = {event};
        siddhiClient.send("output", events);
    }

    public DebsAnormalyDetector(){
        this.siddhiClient = new TcpNettyClient();
       this.siddhiClient.connect("localhost", 8000);

    }
}

