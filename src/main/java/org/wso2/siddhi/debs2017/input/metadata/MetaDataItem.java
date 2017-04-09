package org.wso2.siddhi.debs2017.input.metadata;

/**
 * Holder for single meta-data item
 */
public class MetaDataItem {

    private String machineNumber;
    private int clusterCenters;
    private double probabilityThreshold;
    private String dimension;

    /**
     * The constructor
     *
     * @param machineNumber        uniquely identify the machine
     * @param dimension            property of the machine
     * @param clusterCenters       cluster centers per dimension for the particular machine
     * @param probabilityThreshold threshold to decide whether the sequence is an anomaly
     */
    public MetaDataItem(String machineNumber, String dimension, int clusterCenters,
                        double probabilityThreshold) {
        this.machineNumber = machineNumber;
        this.clusterCenters = clusterCenters;
        this.probabilityThreshold = probabilityThreshold;
        this.dimension = dimension;

    }

    public synchronized int getClusterCenters() {
        return clusterCenters;

    }
    public synchronized double getProbabilityThreshold() {
        return probabilityThreshold;
    }

    public synchronized String getDimension() {
        return dimension;
    }

    public void setDimension(String dimension) {
        this.dimension = dimension;
    }

    /**
     *
     * @return the machine number string
     */
    public String getMachineNumber(){
        return machineNumber;
    }
}