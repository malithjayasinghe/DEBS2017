package org.wso2.siddhi.debs2017.transport.processor;

import com.lmax.disruptor.EventHandler;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.debs2017.transport.TcpNettyClient;

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
    private double probability;
    private double threshold;
    static int count =0;
    @Override
    public void onEvent(EventWrapper wrapper, long l, boolean b) throws Exception {

        //System.out.println(wrapper.getEvent());
        Object[] o = wrapper.getEvent().getData();
         probability = Double.parseDouble(o[3].toString());
         threshold  = Double.parseDouble(o[4].toString());

         if(probability < threshold && probability > 0) {
             System.out.println(probability);
             send(wrapper.getEvent());
         }

    }

    private synchronized void send(Event event) {
        count++;
        System.out.println(count);
        Event [] events = {event};
        siddhiClient.send("output", events);
    }

    public DebsAnormalyDetector(String host, int port){
        this.siddhiClient = new TcpNettyClient();
       this.siddhiClient.connect(host, port);

    }
}

