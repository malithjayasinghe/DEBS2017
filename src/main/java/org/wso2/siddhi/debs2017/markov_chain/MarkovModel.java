package org.wso2.siddhi.debs2017.markov_chain;

import org.wso2.siddhi.query.api.expression.condition.In;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

/**
 * Created by sachini on 1/27/17.
 */
public class MarkovModel {



    private int currentCenter = 0;
    private int previousCenter= 0;
    private int exPrevCenter = 0;
    private int exCurrCenter = 0;

    /**
     * hash map to keep track of the event count of the center transitions
     **/
    private HashMap<Integer, HashMap<Integer,Integer>> transitionEventCount  = new HashMap<>();

    /**
     * arraylist to maintain the current events in the time window for which probability sequence is checked
     */
    private ArrayList<Integer> eventOrder = new ArrayList<>();

    private HashMap<Integer,Integer> totalTransitions;
    private double totalProbability;

    /**
     * update the event count
     * get the number of events transitioned from a partiular center (previous center)
     * increment the event count for the transition from the previous center to current center
     */



    public void updateModel(){

        if(transitionEventCount.containsKey(previousCenter)){

           HashMap<Integer,Integer> temp =  transitionEventCount.get(previousCenter);
           if(temp.containsKey(currentCenter)){
               temp.put(currentCenter,temp.get(currentCenter)+1);
               transitionEventCount.put(previousCenter,temp);
           }else{
               temp.put(currentCenter,1);
               transitionEventCount.put(previousCenter,temp);
           }


        }else {
            HashMap<Integer,Integer> hm = new HashMap<>();
            hm.put(currentCenter, 1);
            transitionEventCount.put(previousCenter, hm);

        }
    }


    /**
     * reduce the event count as events expire from the external time window
     *@param curr the cluster center that is expired
     *@param prev the cluster center that is before the expired center
    **/
    public void reduceCount(int prev, int curr){
        totalTransitions = transitionEventCount.get(prev);
        totalTransitions.put(curr,totalTransitions.get(curr)-1);
        if(totalTransitions.get(curr) == 0)
            totalTransitions.remove(curr);
    }

    /**
     * traverse the arraylist containing the events in the current window
     * get the event counts from the transitionEventCount hashmap
     * calculate the current probability for a particular transition
     * multiply each transition probability to get the total combined probability
     */
    public void updateProbability() {


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

                if (eventCount > 0 && currentEventCount >0) {
                    currentProbability = currentProbability * (currentEventCount / eventCount);
                }
                eventCount = 0;


        }
            totalProbability = currentProbability;
      }

    public double gettotalProbability(){
        return totalProbability;
    }

    public int getPreviousCenter(){return previousCenter;}

    public int getCurrentCenter(){return currentCenter;}

    public void setCurrentCenter(int center){
        currentCenter = center;
    }

    public void setPreviousCenter(int center){ previousCenter = center;}

    public int getExPrevCenter(){return exPrevCenter;}

    public int getExCurrCenter(){return exCurrCenter;}

    public void setExPrevCenter(int center){ exPrevCenter = center;}

    public void setExCurrCenter(int center){exCurrCenter = center; }

    public void setEventOrder(ArrayList<Integer> arr){eventOrder = arr;}


}
