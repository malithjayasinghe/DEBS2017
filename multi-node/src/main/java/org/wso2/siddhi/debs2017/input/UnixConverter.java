package org.wso2.siddhi.debs2017.input;

import java.text.DateFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

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
public class UnixConverter {

    public static long getUnixTime(String data) {
        String source = data.substring(0, 10) + " " + data.substring(11, 19);
        String timeZ = data.substring(19);
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date dateTime;
        df.setTimeZone(TimeZone.getTimeZone(timeZ));
        dateTime = df.parse(source, new ParsePosition(0));
        return new Long(dateTime.getTime()) / 1000;
    }
}
