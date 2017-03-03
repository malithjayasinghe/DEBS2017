package org.wso2.siddhi.debs2017.input;

import java.util.HashMap;

/**
 * Created by sachini on 2/24/17.
 */
public class DebsMetaData  {


    private String machineNumebr;
    private int clusterCenters;
    private int windowLength;
    private double probabilityThreshold;
    private String dimension;
    private String model;

    public String getDimensionState() {
        return dimensionState;
    }

    public void setDimensionState(String dimensionState) {
        this.dimensionState = dimensionState;
    }

    private String dimensionState;

    private String mapKey;

    public   static HashMap<String, DebsMetaData> meta  = new HashMap<>();

    public DebsMetaData(String machineNumebr,String model, String dimension,int clusterCenters,String state,
                        double probabilityThreshold,int windowLength){
        this.model = model;
        this.machineNumebr = machineNumebr;
        this.clusterCenters = clusterCenters;
        this.windowLength = windowLength;
        this.probabilityThreshold = probabilityThreshold;
        this.dimension = dimension;
        this.dimensionState = state;
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
        return machineNumebr;
    }

    public void setMachineNumebr(String machineNumebr) {
        this.machineNumebr = machineNumebr;
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


    public static void storeValues(DebsMetaData dm) {
        String mapKey = dm.getMachineNumebr() + "_" + dm.getDimension();
        meta.put(mapKey, dm);
    }

}
