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

package org.wso2.siddhi.debs2017.markovchain;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @deprecated
 */
public class MarkoveModified {
    private int currentCenter = 0;
    private int previousCenter = 0;
    private int exPrevCenter = 0;
    private int exCurrCenter = 0;

    /**
     * hash map to keep track of the event count of the center transitions
     **/
    private HashMap<Integer, HashMap<Integer, EventDataHolder>> transitionEventCount = new HashMap<>();

    /**
     * arraylist to maintain the current events in the time window for which probability sequence is checked
     */
    private ArrayList<Integer> eventOrder = new ArrayList<>();


    /**
     * hashmap to keep track of the total transitions from a particular cluster center
     */
    private HashMap<Integer, Integer> totalTransitions = new HashMap<>();

    /**
     * the total transitional probability for the events in a particular time window
     */
    private double totalProbability;

    /**
     * update the event count
     * get the number of events transitioned from a partiular center (previous center)
     * increment the event count for the transition from the previous center to current center
     */


    private void updateModel() {


        //updating the total event count
        if (totalTransitions.containsKey(previousCenter)) {
            totalTransitions.put(previousCenter, totalTransitions.get(previousCenter) + 1);
        } else {
            totalTransitions.put(previousCenter, 1);
        }
        //updating the current event
        if (transitionEventCount.containsKey(previousCenter)) {
            //total number of transitions from the particular previous center
            double total = totalTransitions.get(previousCenter);

            //retrieve the hashmap for the transitions starting from the particular cluster center
            HashMap<Integer, EventDataHolder> temp = transitionEventCount.get(previousCenter);
            EventDataHolder eventDataHolder;
            if (temp.containsKey(currentCenter)) {

                //retrieve the EventDataHolder object for the particular event
                 eventDataHolder = temp.get(currentCenter);
                //increment the event count
                eventDataHolder.setEventCount(eventDataHolder.getEventCount() + 1);
                temp.put(currentCenter, eventDataHolder);


            } else {
                /**
                 *   if there is no transiton from the previous center to the current center,create a new eventDataHolder
                 *   object and store it in the hashmap containing transitions from the previous center
                 */
                EventDataHolder newEvent = new EventDataHolder();
                newEvent.setEventCount(1);
                newEvent.setProbability(1);
                temp.put(currentCenter, newEvent);

            }
            /**
             * update the transitional probabilities for each event having transitions from the particualr previous center
             */
            for (Integer key : temp.keySet()) {
                eventDataHolder = temp.get(key);
                eventDataHolder.setProbability(eventDataHolder.getEventCount() / total);
            }
            //store the updated hashmap for transitions from the particular previous center
            transitionEventCount.put(previousCenter, temp);


        } else {
            /**
             * if there are no event transitions from the particular previous center
             * create a new hashmap and add a new event probability object
             */

            EventDataHolder newEvent = new EventDataHolder();
            newEvent.setEventCount(1);
            newEvent.setProbability(1);
            HashMap<Integer, EventDataHolder> hm = new HashMap<>();
            hm.put(currentCenter, newEvent);
            transitionEventCount.put(previousCenter, hm);

        }
    }


    /**
     * reduce the event count as events expire from the external time window
     * update the probability after reducing the event count
     * @param curr the cluster center that is expired
     * @param prev the cluster center that is before the expired center
     **/
    public void reduceCount(int prev, int curr) {
        totalTransitions.put(prev, totalTransitions.get(prev) - 1);
        if (totalTransitions.get(prev) == 0) {
            totalTransitions.remove(prev);
            transitionEventCount.remove(prev);
        } else {
            double total = totalTransitions.get(prev);
            HashMap<Integer, EventDataHolder> eventDataMap = transitionEventCount.get(prev);
            EventDataHolder event = eventDataMap.get(curr);
            event.setEventCount(event.getEventCount() - 1);

            if (event.getEventCount() == 0) {
                eventDataMap.remove(curr);
            }
            for (Integer key : eventDataMap.keySet()) {
                event = eventDataMap.get(key);
                event.setProbability(event.getEventCount() / total);
            }
        }


    }

    /**
     * traverse the arraylist containing the events in the current window
     * get the event counts from the transitionEventCount hashmap
     * calculate the current probability for a particular transition
     * multiply each transition probability to get the total combined probability
     */
    public double updateProbability(ArrayList<Integer> eventOrder) {

        this.eventOrder = eventOrder;
        int currentEvent = 0;
        int previousEvent = 0;
        double eventprobability = 1;
        double transitionalProb;

        int seqprev = eventOrder.size()-2;
        int seqlast = eventOrder.size()-1;
        int eventPrev = eventOrder.get(seqprev);
        int eventLAST = eventOrder.get(seqlast);

        // check whether there are any transitions from the previous center
        if( !totalTransitions.containsKey(eventPrev)){
            totalProbability = 0.0;
        }
        //check whether a transition exist for the last event transition
        else if(transitionEventCount.get(eventPrev).get(eventLAST) == null){
         totalProbability = 0.0;
        }else {


            for (int i = 1; i < eventOrder.size(); i++) {
                if (previousEvent == 0 && currentEvent == 0) {
                    previousEvent = eventOrder.get(0);
                    currentEvent = eventOrder.get(1);
                    EventDataHolder event = transitionEventCount.get(previousEvent).get(currentEvent);
                    transitionalProb = event.getProbability();
                    eventprobability = eventprobability * transitionalProb;
                } else {
                    previousEvent = currentEvent;
                    currentEvent = eventOrder.get(i);
                    EventDataHolder event = transitionEventCount.get(previousEvent).get(currentEvent);
                    transitionalProb = event.getProbability();
                    eventprobability = eventprobability * transitionalProb;
                }



            }

            totalProbability = eventprobability;
        }

        return totalProbability;
    }


    public void execute(int center) {



            /**
             * initialize the current center and previous center
             */

            if (currentCenter == 0 && previousCenter == 0) {

                previousCenter = center;



            } else if (currentCenter == 0) {

                currentCenter = center;
                updateModel();


            } else {
                /**
                 * if both centers are initilaized set the previous center to existing current center
                 * set the current center to the latest event retrieved from the stream
                 * update the event count
                 * calcualte the current probability and return
                 */
                previousCenter = currentCenter;
                currentCenter = center;
                updateModel();

                /**
                 * check whether the added event and the expired event are equal
                 * if so return the same totalprobability value
                 */
                //  if (exPrevCenter == previousCenter && exCurrCenter == currentCenter) {
                //    return totalProbability;
                // } else {

                //  }


            }
        }



        /**
         * remove the expired event from the hashmap maintaining the event count
         *
         * @param curr the expired event from the time window
         * @param prev the event before the expired event
         */

    public void removeEvent(int prev, int curr) {

        exPrevCenter = prev;
        exCurrCenter = curr;


    }


    /**
     * initilaize the transitional probabilities once the window is full
     */
    public void initialize(ArrayList<Integer> centers){
        for(int i=1; i <centers.size(); i++){
            if (currentCenter == 0 && previousCenter == 0) {
                previousCenter = eventOrder.get(0);
                currentCenter = eventOrder.get(1);
                updateModel();
            } else{
                currentCenter =eventOrder.get(i);
                previousCenter = currentCenter;
                updateModel();
            }
        }
    }


}
