package org.wso2.siddhi.debs2017.markov_chain;

import java.util.ArrayList;

/**
 * Created by sachini on 1/30/17.
 */
public class MarkovExecution {

    MarkovModel markovModel = new MarkovModel();
    AnomalyDetection anomalyDetector = new AnomalyDetection(0.5, "_1");


    public double execute(int center, ArrayList<Integer> arr) {



        //INPUT values for each threshold probability and dimension - hardcoded for one dimension
        //DO: IMPLEMENT LOGIC TO READ THE META DATA VALUES

        //read from meta data file
       // int size =

        //pass the parameter from meta data
       // markovModel.setWindowSize(3);

        //set the sequence to be checked for combined probability
         // markovModel.setCheckingSequence(n);

        //passing the event sequence to calculate the probability

        markovModel.setEventOrder(arr);

        if(markovModel.getCurrentCenter()==0 && markovModel.getPreviousCenter() == 0){
            //System.out.println("set initial center");
            markovModel.setPreviousCenter(center);

            return -1;
            //System.out.println(markovModel.getPreviousCenter());
        }
        else if(markovModel.getCurrentCenter() == 0){
            //System.out.println("set initial center");
            markovModel.setCurrentCenter(center);

            //System.out.println(markovModel.getCurrentCenter());
            markovModel.updateModel();
            return -1;
            //System.out.println("update events");
            //markovModel.updateProbability();
        }
      else{
           // System.out.println("Start update");
            markovModel.setPreviousCenter(markovModel.getCurrentCenter());
            //System.out.println(markovModel.getPreviousCenter());
            markovModel.setCurrentCenter(center);

            //System.out.println(markovModel.getCurrentCenter());

            markovModel.updateModel();
            markovModel.updateProbability();
            return markovModel.gettotalProbability();



        }


    }

    public void removeEvent(int prev, int curr){
      //  if(markovModel.getExPrevCenter() == 0 && markovModel.getExCurrCenter() == 0){
           // System.out.println("setting exprev");
            markovModel.setExPrevCenter(prev);
            markovModel.setExCurrCenter(curr);
     //   }else{

           // markovModel.setExPrevCenter(prev);
           // markovModel.setExCurrCenter(curr);
            markovModel.reduceCount(markovModel.getExPrevCenter(),markovModel.getExCurrCenter());
      //  }
    }

}
