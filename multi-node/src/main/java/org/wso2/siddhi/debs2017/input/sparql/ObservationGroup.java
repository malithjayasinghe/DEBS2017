package org.wso2.siddhi.debs2017.input.sparql;

import org.wso2.siddhi.core.event.Event;

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
public class ObservationGroup {
    private long timestamp;
    private ArrayList<Event> dataArr;

    public long getTimestamp() {
        return this.timestamp;
    }

    public void setTimestamp(long time) {
        this.timestamp = time;
    }

    public ArrayList<Event> getDataArr() {
        return this.dataArr;
    }

    public ObservationGroup(long timestamp, ArrayList<Event> arr) {
        this.timestamp = timestamp;
        this.dataArr = arr;
    }
}
