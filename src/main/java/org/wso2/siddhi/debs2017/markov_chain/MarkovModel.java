package org.wso2.siddhi.debs2017.markov_chain;

import org.wso2.siddhi.query.api.expression.condition.In;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

/**
 * Created by sachini on 1/27/17.
 */
public class MarkovModel {


    private  int windowSize;

    private int currentCenter = 0;
    private int previousCenter= 0;
    private int exPrevCenter = 0;
    private int exCurrCenter = 0;
    private HashMap<Integer, HashMap<Integer,Integer>> transitionEventCount  = new HashMap<>();
    private ArrayList<Integer> eventOrder = new ArrayList<>();
    private ArrayList<Integer> getExpiredEvent = new ArrayList<>();
    private HashMap<Integer,Integer> totalTransitions;
    private double totalProbability;
    private int checkingSequence;



    public void updateModel(){
        //updating the event count

        if(transitionEventCount.containsKey(previousCenter)){
           // transitionEventCount.get(previousCenter).put(currentCenter, );
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
           // System.out.println(transitionEventCount.get(previousCenter).get(currentCenter));
        }
    }

    //reduce event count
    public void reduceCount(int prev, int curr){
        totalTransitions = transitionEventCount.get(prev);
        totalTransitions.put(curr,totalTransitions.get(curr)-1);
        if(totalTransitions.get(curr) == 0)
            totalTransitions.remove(curr);
    }

    
    public void updateProbability() {


        double currentProbability = 1;
        int currentEvent = 0;
        int previousEvent = 0;
        double eventCount = 0;

       // if (eventOrder.size() >= windowSize) {

            for (int i = 1; i < eventOrder.size(); i++) {

                if (previousEvent == 0 && currentEvent == 0) {
                    previousEvent = eventOrder.get(0);
                    currentEvent = eventOrder.get(1);
                } else {
                    previousEvent = currentEvent;
                    currentEvent = eventOrder.get(i);
                }
                totalTransitions = transitionEventCount.get(previousEvent);
                double currentEventCount = transitionEventCount.get(previousEvent).get(currentEvent);
                for (Integer key : totalTransitions.keySet()) {
                    // System.out.println(totalTransitions.get(key));
                    eventCount = eventCount + totalTransitions.get(key);
                    // System.out.println(eventCount + " "+ "totalTransitionsfrom"+ " "+ previousCenter);
                }
                if (eventCount > 0 && currentEventCount >0) {
                    currentProbability = currentProbability * (currentEventCount / eventCount);
                }
                eventCount = 0;

           // }
        }
            totalProbability = currentProbability;
           // System.out.println(currentProbability);
           // System.out.println(totalProbability);



    }

    public void addEvents(int center){

        //to keep track of the events to remove
        getExpiredEvent.add(center);

        //to keep track of the event sequnece so far
        eventOrder.add(center);
       if(eventOrder.size()-checkingSequence == 1){
//            reduceCount(getExpiredEvent.get(0),getExpiredEvent.get(1));
//            getExpiredEvent.remove(0);
           eventOrder.remove(0);
        }
        //System.out.println(eventOrder);
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

    public  int getWindowSize(){return  windowSize;}

    public  void setWindowSize(int size){this.windowSize = size;}

    public void setCheckingSequence(int n)  { this.checkingSequence = n;}
}
