package org.wso2.siddhi.debs2017.kmeans;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

/*
* Copyright (c) 2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
public class Clusterer {

    private int noOfClusters;
    private int maxIter;
    private ArrayList<Double> data;
    private int dataSize;
    private ArrayList<Double>[] clusterGroup;
    private ArrayList<Double> center;
    private ArrayList<Double> centerOld;



    public Clusterer(int noOfClusters, int maxIter, ArrayList<Double> dataSet) {

        this.noOfClusters = noOfClusters;
        this.maxIter = maxIter;
        this.data = dataSet;

        this.center = new ArrayList<>();
        this.centerOld = new ArrayList<>();


        //check if the no. of data points are less than the no. of clusters
        this.dataSize = data.size();
        if (this.noOfClusters > dataSize) {
            this.noOfClusters = dataSize;
        }

        this.clusterGroup = new ArrayList[this.noOfClusters];
        //get the first k distinct values from the data. This will be used to initialize the cluster centers

        int distinctCount =0;
        ArrayList<Double> distinctValues = new ArrayList<>();
        for (int i = 0; i < dataSize; i++) {
            if (!distinctValues.contains(data.get(i))) {

                distinctValues.add(data.get(i));
                center.add(data.get(i));
                this.clusterGroup[distinctCount] = new ArrayList<>();
                distinctCount++;

            }
            if (center.size() == this.noOfClusters) {
                break;
            }
        }


        //for (int i =0; i<this.noOfClusters; i++){
           // this.clusterGroup[i] = new ArrayList<>();
        //}
        // if k values are not found, k should be the same as the no of distinct values found
        this.noOfClusters = center.size();
    }


    /**
     * Perform clustering
     *
     */
    public void cluster() {
        int iter = 0;

        do {
            assignToCluster();
            reinitializeCluster();
            if (!center.equals(centerOld)) {
                for (int i = 0; i < clusterGroup.length; i++) {
                    clusterGroup[i] = new ArrayList<>();

                }
            }
            iter++;

        } while (!center.equals(centerOld) && iter < maxIter);
    }

    /**
     * reinitialize the cluster centres and store the old ones
     *
     */
    private void reinitializeCluster() {
        for (int i = 0; i < noOfClusters; i++) {
            centerOld.add(i, center.get(i));
            if (!clusterGroup[i].isEmpty()) {
                center.set(i, average(clusterGroup[i]));
            }
        }
    }

    /**
     * base on the data points assigned to the cluster, recalculates the cluster center
     *
     * @param doubles : the cluster
     * @return : the new cluster center
     */
    private Double average(ArrayList<Double> doubles) {
        double sum = 0;
        for (int i = 0; i < doubles.size(); i++) {
            sum = sum + doubles.get(i);
        }
        return (sum / doubles.size());
    }

    /**
     * calculates the nearest center to each data point and adds the data to the cluster of respective center
     */
    private void assignToCluster() {

        double[] difference;
        double dataItem, cenVal, diff;

        for (int i = 0; i < dataSize; i++) {
            difference = new double[noOfClusters];
            dataItem = data.get(i);
            for (int j = 0; j < noOfClusters; j++) {
                cenVal = center.get(j);
                diff = Math.abs(cenVal - dataItem);
                difference[j] = diff;

            }
            int minIndex = getMinIndex(difference);


            clusterGroup[minIndex].add(dataItem);

        }
    }


    /**
     * calculates distance between each data point and cluster and returns the closest center
     *
     * @param diff: the distance to each cluster center for a data point
     * @return : the closest cluster
     */
    private int getMinIndex(double[] diff) {
        int minIndex = 0;

        for (int i = 1; i < noOfClusters; i++) {
            if (diff[minIndex] > diff[i]) {
                minIndex = i;
            }

        }

        return minIndex;
    }

    /**
     * gives the cluster center the last data point belongs to
     *
     * @return : center
     */
    public int getCenter(double value) {
        double[] minDist = new double[noOfClusters];


        //sort the arraylist
        Collections.sort(center);

        //get distance to each
        for (int i = 0; i < noOfClusters; i++) {
            minDist[i] = (Math.abs(center.get(i) - value));

        }

        return getMinIndex(minDist) + 1;

    }

    public int[] getCenter(ArrayList<Double> data){
        int [] output = new int[data.size()];
        double[] minDist = new double[noOfClusters];

        for(int i=0; i<data.size(); i++){

            for (int j = 0; j < noOfClusters; j++) {
                minDist[j] = (Math.abs(center.get(j) - data.get(i)));

            }

            output[i] = getMinIndex(minDist) +1;
        }

        return output;
    }
}
