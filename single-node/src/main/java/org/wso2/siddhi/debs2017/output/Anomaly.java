package org.wso2.siddhi.debs2017.output;

import com.hp.hpl.jena.rdf.model.Model;
import org.wso2.siddhi.core.event.Event;

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
public class Anomaly implements Comparable<Anomaly>{


    private int identifier;
    private Event event;

    public int getIdentifier() {
        return this.identifier;
    }

    public Event getEvent() {
        return this.event;
    }


    public Anomaly(int identifier, Event event) {
        this.identifier = identifier;
        this.event = event;
    }



    @Override
    public int compareTo(Anomaly anomaly) {
        if(this.identifier==anomaly.identifier) {
            return 0;
        }else if(this.identifier>anomaly.identifier) {
            return 1;
        }else {
            return -1;
        }
    }
}

