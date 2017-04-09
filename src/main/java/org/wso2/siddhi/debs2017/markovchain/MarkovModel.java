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
 * Created by sachini on 1/27/17.
 * @deprecated
 */
public class MarkovModel {


    private int currentCenter = 0;
    private int previousCenter = 0;
    private int exPrevCenter = 0;
    private int exCurrCenter = 0;

    /**
     * hash map to keep track of the event count of the center transitions
     **/
    private HashMap<Integer, HashMap<Integer, Integer>> transitionEventCount = new HashMap<>();

    /**
     * arraylist to maintain the current events in the time window for which probability sequence is checked
     */
    private ArrayList<Integer> eventOrder = new ArrayList<>();

    private HashMap<Integer, Integer> totalTransitions;
    private double totalProbability;

    /**
     * update the event count
     * get the number of events transitioned from a partiular center (previous center)
     * increment the event count for the transition from the previous center to current center
     */


    private void updateModel() {

        if (transitionEventCount.containsKey(previousCenter)) {

            HashMap<Integer, Integer> temp = transitionEventCount.get(previousCenter);
            if (temp.containsKey(currentCenter)) {
                temp.put(currentCenter, temp.get(currentCenter) + 1);
                transitionEventCount.put(previousCenter, temp);
            } else {
                temp.put(currentCenter, 1);
                transitionEventCount.put(previousCenter, temp);
            }


        } else {
            HashMap<Integer, Integer> hm = new HashMap<>();
            hm.put(currentCenter, 1);
            transitionEventCount.put(previousCenter, hm);

        }
    }


    /**
     * reduce the event count as events expire from the external time window
     *
     * @param curr the cluster center that is expired
     * @param prev the cluster center that is before the expired center
     **/
    private void reduceCount(int prev, int curr) {
        totalTransitions = transitionEventCount.get(prev);
        totalTransitions.put(curr, totalTransitions.get(curr) - 1);
        if (totalTransitions.get(curr) == 0)
            totalTransitions.remove(curr);
    }

    /**
     * traverse the arraylist containing the events in the current window
     * get the event counts from the transitionEventCount hashmap
     * calculate the current probability for a particular transition
     * multiply each transition probability to get the total combined probability
     */
    private void updateProbability() {


        double currentProbability = 1;
        int currentEvent = 0;
        int previousEvent = 0;
        double eventCount = 0;


        for (int i = 1; i < eventOrder.size(); i++) {

            if (previousEvent == 0 && currentEvent == 0) {
                previousEvent = eventOrder.get(0);
                currentEvent = eventOrder.get(1);
            } else {
                previousEvent = currentEvent;
                currentEvent = eventOrder.get(i);
            }
            //get the event count for  the transitions between two particular centers
            double currentEventCount = transitionEventCount.get(previousEvent).get(currentEvent);

            // retreive the hashmap to get all the transitions from a particular center
            totalTransitions = transitionEventCount.get(previousEvent);

            // traverse the retrieved hashmap to get the count of all transitions from a particular center
            for (Integer key : totalTransitions.keySet()) {
                eventCount = eventCount + totalTransitions.get(key);
            }

            if (eventCount > 0 && currentEventCount > 0) {
                currentProbability = currentProbability * (currentEventCount / eventCount);
            }
            eventCount = 0;


        }
        totalProbability = currentProbability;
    }

    private double gettotalProbability() {
        return totalProbability;
    }

    private int getPreviousCenter() {
        return previousCenter;
    }

    private int getCurrentCenter() {
        return currentCenter;
    }

    private void setCurrentCenter(int center) {
        currentCenter = center;
    }

    private void setPreviousCenter(int center) {
        previousCenter = center;
    }

    private int getExPrevCenter() {
        return exPrevCenter;
    }

    private int getExCurrCenter() {
        return exCurrCenter;
    }

    private void setExPrevCenter(int center) {
        exPrevCenter = center;
    }

    private void setExCurrCenter(int center) {
        exCurrCenter = center;
    }

    private void setEventOrder(ArrayList<Integer> arr) {
        eventOrder = arr;
    }


    public  double execute(int center, ArrayList<Integer> arr){
        /**
         *  passing the event sequence in the current time window  to calculate the  combined probability
         * */
        setEventOrder(arr);


        /**
         * initialize the current center and previous center
         *
         * */

        if (getCurrentCenter() == 0 && getPreviousCenter() == 0) {

            setPreviousCenter(center);

            return -1;

        } else if (getCurrentCenter() == 0) {

            setCurrentCenter(center);
            updateModel();
            return 1;

        } else {
            /**
             * if both centers are initilaized set the previous center to existing current center
             * set the current center to the latest event retrieved from the stream
             * update the event count
             * calcualte the current probability and return
             * */

            setPreviousCenter(getCurrentCenter());
            setCurrentCenter(center);
            updateModel();
            updateProbability();
            return gettotalProbability();


        }
    }

    /**
     * remove the expired event from the hashmap maintaining the event count
     *
     * @param curr the expired event from the time window
     * @param prev the event before the expired event
     */

    public void removeEvent(int prev, int curr) {

        setExPrevCenter(prev);
        setExCurrCenter(curr);
        reduceCount(getExPrevCenter(), getExCurrCenter());

    }








}
