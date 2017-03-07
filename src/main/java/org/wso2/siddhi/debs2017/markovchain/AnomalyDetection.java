package org.wso2.siddhi.debs2017.markovchain;

import java.util.HashMap;

/**
 * Created by sachini on 1/27/17.
 * NOT USED
 */
public class AnomalyDetection {
    private HashMap<String, Double> thresholdPerDimension = new HashMap<>();


    public AnomalyDetection(Double thresholdProbability, String dimension){
        thresholdPerDimension.put(dimension , thresholdProbability);
    }

    public boolean checkThreshold(double currentProbability, String dimension){
        if(thresholdPerDimension.get(dimension)< currentProbability){
            return true;
        }else
            return false;
    }

    public String generateAlert(){
        //System.out.println("Anomaly");
        return "anomaly";
    }



}
