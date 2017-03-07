package org.wso2.siddhi.debs2017.input;

import java.util.HashMap;

/**
 * Created by sachini on 2/24/17.
 */


/**
 * class to extract the meta data for the machines
 */
public class DebsMetaData  {


    private String machineNumber;
    private int clusterCenters;
    private int windowLength;
    private double probabilityThreshold;
    private String dimension;

    /**
     * the type of machine i.e molding or assembley
     **/
    private String model;




    /**
     * hashmap to store the meatdata
     * key - combination of machine number anfd dimension
     * value DebsMetaData object
     */

    public   static HashMap<String, DebsMetaData> meta  = new HashMap<>();


    /**
     * @param machineNumber uniquely identify the machine
     * @param model     type of machine i.e molding or injecting
     * @param dimension      property of the machine
     * @param clusterCenters  cluster centers per dimension for the particular machine

     * @param probabilityThreshold threshold to decide whether the sequence is an anomaly
     * @param windowLength      time duartion for which the window exist
     */

    public DebsMetaData(String machineNumber,String model, String dimension,int clusterCenters,
                        double probabilityThreshold,int windowLength){
        this.model = model;
        this.machineNumber = machineNumber;
        this.clusterCenters = clusterCenters;
        this.windowLength = windowLength;
        this.probabilityThreshold = probabilityThreshold;
        this.dimension = dimension;

    }


    /**
     * @param dm the DebsMetaData object to be store din the hashmap
     * store the metadata object generated
     */

    public static void storeValues(DebsMetaData dm) {
        String mapKey = dm.getMachineNumebr() + "_" + dm.getDimension();
        meta.put(mapKey, dm);
    }

    public int getClusterCenters() {
        return clusterCenters;
    }

    public void setClusterCenters(int clusterCenters) {
        this.clusterCenters = clusterCenters;
    }

    public int getWindowLength() {
        return windowLength;
    }

    public void setWindowLength(int windowLength) {
        this.windowLength = windowLength;
    }

    public double getProbabilityThreshold() {
        return probabilityThreshold;
    }

    public void setProbabilityThreshold(double probabilityThreshold) {
        this.probabilityThreshold = probabilityThreshold;
    }

    public String getMachineNumebr() {
        return machineNumber;
    }

    public void setMachineNumebr(String machineNumebr) {
        this.machineNumber = machineNumebr;
    }

    public String getDimension() {
        return dimension;
    }

    public void setDimension(String dimension) {
        this.dimension = dimension;
    }

    public String getModel() {
        return model;
    }

    public void setModel(String model) {
        this.model = model;
    }




}
