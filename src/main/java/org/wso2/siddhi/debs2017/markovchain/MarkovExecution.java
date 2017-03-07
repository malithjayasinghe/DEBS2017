package org.wso2.siddhi.debs2017.markovchain;

import java.util.ArrayList;

/**
 * Created by sachini on 1/30/17.
 */
public class MarkovExecution {

    MarkovModel markovModel = new MarkovModel();

    /**
     * @param center current cluster center retreived from the stream
     * @param arr    arraylist containing the event transition for the current window
     */

    public double execute(int center, ArrayList<Integer> arr) {

        /**
         *  passing the event sequence in the current time window  to calculate the  combined probability
         * */
        markovModel.setEventOrder(arr);


        /**
         * initialize the current center and previous center
         *
         * */

        if (markovModel.getCurrentCenter() == 0 && markovModel.getPreviousCenter() == 0) {

            markovModel.setPreviousCenter(center);

            return -1;

        } else if (markovModel.getCurrentCenter() == 0) {

            markovModel.setCurrentCenter(center);
            markovModel.updateModel();
            return -1;

        } else {
            /**
             * if both centers are initilaized set the previous center to existing current center
             * set the current center to the latest event retrieved from the stream
             * update the event count
             * calcualte the current probability and return
             * */

            markovModel.setPreviousCenter(markovModel.getCurrentCenter());
            markovModel.setCurrentCenter(center);
            markovModel.updateModel();
            markovModel.updateProbability();
            return markovModel.gettotalProbability();


        }


    }

    /**
     * remove the expired event from the hashmap maintaining the event count
     * @param curr the expired event from the time window
     * @param prev the event before the expired event
     */

    public void removeEvent(int prev, int curr) {

        markovModel.setExPrevCenter(prev);
        markovModel.setExCurrCenter(curr);
        markovModel.reduceCount(markovModel.getExPrevCenter(), markovModel.getExCurrCenter());

    }

}
