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

package org.wso2.siddhi.debs2017.input;

import java.util.HashMap;


/**
 * class to extract the meta data for the machines
 */
public class DebsMetaData {


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

    public static HashMap<String, DebsMetaData> meta = new HashMap<>();


    /**
     * @param machineNumber        uniquely identify the machine
     * @param model                type of machine i.e molding or injecting
     * @param dimension            property of the machine
     * @param clusterCenters       cluster centers per dimension for the particular machine
     * @param probabilityThreshold threshold to decide whether the sequence is an anomaly
     * @param windowLength         time duartion for which the window exist
     */

    public DebsMetaData(String machineNumber, String model, String dimension, int clusterCenters,
                        double probabilityThreshold, int windowLength) {
        this.model = model;
        this.machineNumber = machineNumber;
        this.clusterCenters = clusterCenters;
        this.windowLength = windowLength;
        this.probabilityThreshold = probabilityThreshold;
        this.dimension = dimension;

    }


    /**
     * @param dm the DebsMetaData object to be store din the hashmap
     *           store the metadata object generated
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
