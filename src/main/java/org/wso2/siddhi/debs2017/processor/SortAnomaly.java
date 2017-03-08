/*
 *
 *  Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * /
 *
 */
/**
 * NOT USED
 */


package org.wso2.siddhi.debs2017.processor;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;


public class SortAnomaly {
    List<DebsOutputEvent> integrated = new ArrayList<>();
    public static synchronized void sortList(long id){
        String threadName = "lh"+(id-1);


    }

    public static synchronized void printList(List<DebsOutputEvent> debsOut){
        if(debsOut.size() == 5) {
            ListIterator<DebsOutputEvent> li = debsOut.listIterator();
            while (li.hasNext()) {
                System.out.println(li.next().getMachine() + li.next().getDimension() + li.next().gettStanmp());
                //int index = debsOut.indexOf(d);
                System.out.println("---------------------------");
                li.remove();
            }
        }

    }
}
