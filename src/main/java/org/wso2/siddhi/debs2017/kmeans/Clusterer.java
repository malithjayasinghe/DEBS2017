package org.wso2.siddhi.debs2017.kmeans;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;

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
    private double[] center;
    private double[] centerOld;
    private ArrayList<Double> kDist;

    /**
     * The constructor
     *
     * @param numberOfCulsters The number of clusters
     * @param maxIter maximum number of iterations to be used
     * @param dataSet the array containing the values to be clustered
     */
    public Clusterer(int numberOfCulsters, int maxIter, ArrayList<Double> dataSet) {

        this.noOfClusters = numberOfCulsters;
        this.maxIter = maxIter;
        this.data = dataSet;


        //check if the no. of data points are less than the no. of clusters
        this.dataSize = data.size();

        if (this.noOfClusters > dataSize) {
            this.noOfClusters = dataSize;
        }

        //get the first k distinct values from the data. This will be used to initialize the cluster centers
        int kCount = 0;

        ArrayList<Double> kDist = new ArrayList<>();

        for (int i = 0; i < dataSize; i++) {
            if (!kDist.contains(data.get(i))) {
                kDist.add(data.get(i));
                kCount++;
            }
            if (kCount == noOfClusters) {
                break;
            }
        }


        // if k values are not found, k should be the same as the no of distinct values found
        if (kCount < noOfClusters) {
            noOfClusters = kCount;
        }

        this.clusterGroup = new ArrayList[noOfClusters];
        this.center = new double[noOfClusters];
        this.centerOld = new double[noOfClusters];


        //initialize the cluster centers to first k distinct value and creating the groups to hold all cluster values.
        for (int i = 0; i < noOfClusters; i++) {
            this.center[i] = kDist.get(i);
            this.clusterGroup[i] = new ArrayList<>();
        }


    }

    /**
     * Perform clustering
     *
     */
    public void cluster() {
        int iter = 0;
        do {
            assignToCluster();
            reinitializeCluster(iter);
            if (!Arrays.equals(center, centerOld)) {
                for (int i = 0; i < clusterGroup.length; i++) {
                    clusterGroup[i].removeAll(clusterGroup[i]);

                }
            }
            iter++;
        } while (!Arrays.equals(center, centerOld) && iter < maxIter);
    }

    /**
     * reinitialize the cluster centres and store the old ones
     *
     * @param iter : current no. of iterations
     */
    private void reinitializeCluster(int iter) {
        for (int i = 0; i < noOfClusters; i++) {
            centerOld[i] = center[i];
            if (!clusterGroup[i].isEmpty()) {
                center[i] = average(clusterGroup[i]);
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
                cenVal = center[j];
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
        //sort the array
        Arrays.sort(center);
        //get distance to each
        for (int i = 0; i < noOfClusters; i++) {
            minDist[i] = (Math.abs(center[i] - value));

        }
        return getMinIndex(minDist) + 1;
    }
}
