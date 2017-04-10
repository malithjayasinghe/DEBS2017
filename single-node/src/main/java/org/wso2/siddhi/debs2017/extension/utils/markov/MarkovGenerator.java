package org.wso2.siddhi.debs2017.extension.utils.markov;

import java.util.ArrayList;
import java.util.HashMap;

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
public class MarkovGenerator {

    private int currentCenter = 0;
    private int previousCenter = 0;


    /**
     * hash map to keep track of the event count of the center transitions
     **/
    private HashMap<Integer, HashMap<Integer, EventDataHolder>> transitionEventCount = new HashMap<>();

    /**
     * arraylist to maintain the current events in the time window for which probability sequence is checked
     */
    private ArrayList<Integer> eventOrder = new ArrayList<>();
    //private int[] eventOrder;

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


    public void updateModel() {

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
     * traverse the arraylist containing the events in the current window
     * get the event counts from the transitionEventCount hashmap
     * calculate the current probability for a particular transition
     * multiply each transition probability to get the total combined probability
     */
    public double updateProbability(ArrayList<Integer> arr) {
        eventOrder = arr;
        int currentEvent = 0;
        int previousEvent = 0;
        double eventProbability = 1;
        double transitionalProb;
        for (int i = eventOrder.size() - 5; i < eventOrder.size(); i++) {
            if (previousEvent == 0 && currentEvent == 0) {
                currentEvent = eventOrder.get(i);
                previousEvent = eventOrder.get(i - 1);
                EventDataHolder event = transitionEventCount.get(previousEvent).get(currentEvent);
                transitionalProb = event.getProbability();
                eventProbability = eventProbability * transitionalProb;
            } else {
                previousEvent = currentEvent;
                currentEvent = eventOrder.get(i);
                EventDataHolder event = transitionEventCount.get(previousEvent).get(currentEvent);
                transitionalProb = event.getProbability();
                eventProbability = eventProbability * transitionalProb;
            }

        }

        totalProbability = eventProbability;
        return totalProbability;
    }


    public void execute(ArrayList<Integer> arr) {
        /**
         *  passing the event sequence in the current time window  to calculate the  combined probability
         */

        eventOrder = arr;
        /**
         * clearing the hashmaps to keep track of the cluster center transitions of the current window
         */
        transitionEventCount.clear();
        totalTransitions.clear();
        previousCenter = 0;
        currentCenter = 0;
        for (int i = 1; i < eventOrder.size(); i++) {
            /**
             * initialize the current center and previous center
             */

            if (currentCenter == 0 && previousCenter == 0) {
                previousCenter = eventOrder.get(0);
                currentCenter = eventOrder.get(1);
                updateModel();
            } else {
                previousCenter = currentCenter;
                currentCenter = eventOrder.get(i);
                updateModel();
            }
        }
    }

}
