package org.wso2.siddhi.debs2017.transport.test;

import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.debs2017.input.SiddhiDataPublisher;
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
public class TestClient {
   /* public static void main(String[] args) {
        TcpNettyClient siddhiClient = new TcpNettyClient();
        TcpNettyClient siddhiClient1 = new TcpNettyClient();
        TcpNettyClient siddhiClient2 = new TcpNettyClient();
        siddhiClient.connect("localhost", 8080);
        siddhiClient1.connect("localhost", 8080);
        siddhiClient2.connect("localhost", 8080);

        ArrayList<Event> arrayList= new ArrayList<>();
            for (int j = 0; j < 5; j++) {
                arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"WSO2", "1", "2"}));
                arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"IBM", "1", "2"}));
            }


        siddhiClient.send("test", arrayList.toArray(new Event[10]));
        siddhiClient1.send("test", arrayList.toArray(new Event[10]));
        siddhiClient2.send("test", arrayList.toArray(new Event[10]));





    }*/
}
