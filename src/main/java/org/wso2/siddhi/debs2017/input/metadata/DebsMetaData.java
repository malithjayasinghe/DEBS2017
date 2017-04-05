/*
 *
 *  Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * /
 *
 */

package org.wso2.siddhi.debs2017.input.metadata;

import java.util.HashMap;


/**
 * class to extract the meta data for the machines
 */
public class DebsMetaData {


    private String machineNumber;
    private int clusterCenters;
    private double probabilityThreshold;
    private String dimension;

    /**
     * hashmap to store the meatdata
     * key - combination of machine number and dimension
     * value DebsMetaData object
     */

    public static HashMap<String, DebsMetaData> meta = new HashMap<>();


    /**
     * @param machineNumber        uniquely identify the machine
     * @param dimension            property of the machine
     * @param clusterCenters       cluster centers per dimension for the particular machine
     * @param probabilityThreshold threshold to decide whether the sequence is an anomaly
     *
     */

    public DebsMetaData(String machineNumber, String dimension, int clusterCenters,
                        double probabilityThreshold) {
        this.machineNumber = machineNumber;
        this.clusterCenters = clusterCenters;
        this.probabilityThreshold = probabilityThreshold;
        this.dimension = dimension;

    }

    /**
     * @param dm the DebsMetaData object to be store din the hashmap
     *           store the metadata object generated
     */

    public static void storeValues(DebsMetaData dm) {
        String mapKey = dm.getMachineNumebr() + dm.getDimension();
        meta.put(mapKey, dm);
    }

    public synchronized int getClusterCenters() {
        return clusterCenters;
    }

    public void setClusterCenters(int clusterCenters) {
        this.clusterCenters = clusterCenters;
    }

    public synchronized double getProbabilityThreshold() {
        return probabilityThreshold;
    }

    public void setProbabilityThreshold(double probabilityThreshold) {
        this.probabilityThreshold = probabilityThreshold;
    }

    public synchronized String getMachineNumebr() {
        return machineNumber;
    }

    public void setMachineNumebr(String machineNumebr) {
        this.machineNumber = machineNumebr;
    }

    public synchronized String getDimension() {
        return dimension;
    }

    public void setDimension(String dimension) {
        this.dimension = dimension;
    }



}
