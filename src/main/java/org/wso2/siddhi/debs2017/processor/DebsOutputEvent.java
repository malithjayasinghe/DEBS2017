package org.wso2.siddhi.debs2017.processor;

import java.util.Comparator;

/**
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
* @deprecated
*/

public class DebsOutputEvent {

    private String machine;
    private String tStanmp;
    private String dimension;
    private String time;
    private String probability;

    public DebsOutputEvent(){}


   public DebsOutputEvent(String timeStamp,String machine, String dimension, String probability, String time){
       this.probability = probability;
       this.time = time;
       this.machine = machine;
       this.tStanmp =timeStamp;
       this.dimension = dimension;
   }

    public String getMachine() {
        return machine;
    }

    public void setMachine(String machine) {
        this.machine = machine;
    }

    public String gettStanmp() {
        return tStanmp;
    }

    public void settStanmp(String tStanmp) {
        this.tStanmp = tStanmp;
    }

    public String getDimension() {
        return dimension;
    }

    public void setDimension(String dimension) {
        this.dimension = dimension;
    }

    public String getProbability() {
        return probability;
    }

    public void setProbability(String probability) {
        this.probability = probability;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }



  //  @Override
   /* public int compare(DebsOutputEvent o1, DebsOutputEvent o2) {
       int t1 =  Integer.parseInt(o1.gettStanmp().substring(10));
       int t2 =  Integer.parseInt(o2.gettStanmp().substring(10));

       if(t1==t2){
           int m1 = Integer.parseInt(o1.getMachine().substring(15));
           int m2 = Integer.parseInt(o2.getMachine().substring(15));
           if(m1==m2){
               int d1 = Integer.parseInt(o1.getDimension().substring(1));
               int d2 = Integer.parseInt(o2.getDimension().substring(1));
               return d1-d2;
           }
           return m1-m2;
       }
        return t1-t2;
    }*/
}
