package org.wso2.siddhi.debs2017.processor;

import com.lmax.disruptor.EventHandler;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.debs2017.input.sparql.RabbitMessage;
import org.wso2.siddhi.debs2017.output.AlertGenerator;

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
public class DebsAnomalyDetector implements EventHandler<RabbitMessage> {

    private static AlertGenerator alertGenerator;
    private double probability;
    private double threshold;
    static int count = 0;

    @Override
    public void onEvent(RabbitMessage wrapper, long l, boolean b) throws Exception {
        if(wrapper.isStateful()) {
            Object[] o = wrapper.getEvent().getData();
            if (wrapper.getEvent().getTimestamp() == -1l) {
                System.out.println("Termination published");
                alertGenerator.terminate();
            } else {
                probability = Double.parseDouble(o[3].toString());
                threshold = Double.parseDouble(o[4].toString());
                if (probability < threshold && probability > 0) {
                    System.out.println(wrapper.getEvent() + "anomaly--------------"+ "\t"+ l);

                    send(wrapper.getEvent());
                }
            }
        }

    }

    /**
     * Generates an alert
     * @param event the event to generate the alert from
     */
    private synchronized void send(Event event) {
        count++;
        alertGenerator.generateAlert(event);
    }

    /**
     * The constructor
     *
     * @param alertGenerator the alert generator object
     */
    public DebsAnomalyDetector(AlertGenerator alertGenerator) {
        this.alertGenerator = alertGenerator;
    }
}
