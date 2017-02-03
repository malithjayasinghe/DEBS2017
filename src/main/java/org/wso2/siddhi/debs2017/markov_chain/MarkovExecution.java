package org.wso2.siddhi.debs2017.markov_chain;

import java.util.ArrayList;

/**
 * Created by sachini on 1/30/17.
 */
public class MarkovExecution {

    MarkovModel markovModel = new MarkovModel();
    AnomalyDetection anomalyDetector = new AnomalyDetection(0.5, "_1");
    String anomaly;

    public String execute(int center) {



        //INPUT values for each threshold probability and dimension - hardcoded for one dimension
        //DO: IMPLEMENT LOGIC TO READ THE META DATA VALUES

        //read from meta data file
       // int size =

        //pass the parameter from meta data
       // markovModel.setWindowSize(3);

        if(markovModel.getCurrentCenter()==0 && markovModel.getPreviousCenter() == 0){
            //System.out.println("set initial center");
            markovModel.setPreviousCenter(center);
            markovModel.addEvents(center);
            //System.out.println(markovModel.getPreviousCenter());
        }
        else if(markovModel.getCurrentCenter() == 0){
            //System.out.println("set initial center");
            markovModel.setCurrentCenter(center);
            markovModel.addEvents(center);
            //System.out.println(markovModel.getCurrentCenter());
            markovModel.updateModel();
            //System.out.println("update events");
            //markovModel.updateProbability();
        }
      else{
           // System.out.println("Start update");
            markovModel.setPreviousCenter(markovModel.getCurrentCenter());
            //System.out.println(markovModel.getPreviousCenter());
            markovModel.setCurrentCenter(center);
            markovModel.addEvents(center);
            //System.out.println(markovModel.getCurrentCenter());

            markovModel.updateModel();
            markovModel.updateProbability();

            //checking against threshold probability
            if (markovModel.gettotalProbability()< 0.5){
               anomaly =  anomalyDetector.generateAlert() + " " + markovModel.gettotalProbability() ;
            }
            else {

                anomaly = "Normal" + " " + markovModel.gettotalProbability();
                //System.out.println(markovModel.gettotalProbability());
            }

        }
        return anomaly;

    }

}
