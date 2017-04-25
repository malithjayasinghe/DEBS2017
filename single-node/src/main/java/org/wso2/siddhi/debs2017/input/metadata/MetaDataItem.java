package org.wso2.siddhi.debs2017.input.metadata;

/**
 * Created by temp on 4/9/17.
 */
public class MetaDataItem {

    private int clusterCenters;
    private double probabilityThreshold;
    private String dimension;

    /**
     * The constructor
     *
     * @param dimension            property of the machine
     * @param clusterCenters       cluster centers per dimension for the particular machine
     * @param probabilityThreshold threshold to decide whether the sequence is an anomaly
     */
    public MetaDataItem(String dimension, int clusterCenters,
                        double probabilityThreshold) {

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


}
