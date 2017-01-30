package org.wso2.siddhi.debs2017.kmeans;

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

public class WSO2KmeansClustering {



    private int k;
    private int maxIter;
    private ArrayList<Double> data = new ArrayList<>();
    private ArrayList<Double>[] clusterGroup;
    private double[] center;
    private double[] centerOld;


    /**Constructor
     *
     * @param c : the no of cluster centers
     * @param max : the maximum no of iterations
     * @param dataSet : the data set to be clustered
     */
    public WSO2KmeansClustering(int c, int max, ArrayList<Double> dataSet ){

        this.k = c;
        this.maxIter = max;
        this.data = dataSet;
        this.clusterGroup = new ArrayList[k];
        this.center = new double[k];
        this.centerOld =  new double[k];

        if(k > data.size()){
           this.k = data.size();
        }
        createGroup();
       // System.out.println("No of Clusters :"+this.k+"\nNo of clustergroups :"+this.clusterGroup.size());

        initializeCenters();
        /*System.out.println("\nInitial center values");
        for (int i=0; i<center.size(); i++){
            System.out.println("c"+i+":"+center.get(i));
        }*/

        int iter = 0;
        do {

            assignToCluster();
            /*System.out.println("\nChecking each cluster");
            for (int i=0; i<clusterGroup.size(); i++){
                System.out.println("clusterG "+i+":"+clusterGroup.get(i));
            };

            for (int i = 0; i < center.size(); i++) {
                System.out.println("New C" + (i) + " " + center.get(i));
            }*/

            reinitializeCluster(iter);

            //if (!center.equals(centerOld)) {
            if (!Arrays.equals(center, centerOld)) {
                for (int i = 0; i < clusterGroup.length; i++) {
                    clusterGroup[i].removeAll(clusterGroup[i]);
                }
            }
            iter++;
        } while (!center.equals(centerOld) && iter<=maxIter);

        //System.out.println("Iterations : "+iter);


    }

    /**
     * reinitialize the cluster centres and store the old ones
     * @param iter : current no. of iterations
     */
    private void reinitializeCluster(int iter) {
        for (int i = 0; i < k; i++) {
            if (iter == 0) {
                centerOld[i] = center[i];
            } else {
                centerOld[i] = center[i];
            }
            if (!clusterGroup[i].isEmpty()) {
                center[i] = average(clusterGroup[i]);
            }
        }
    }

    /**
     * base on the data points assigned to the cluster, recalculates the cluster center
     * @param doubles : the cluster
     * @return : the new cluster center
     */
    private Double average(ArrayList<Double> doubles) {
        double sum = 0;
        for (int i =0; i<doubles.size(); i++) {
            sum = sum +doubles.get(i);
        }
        return (sum/doubles.size());
    }

    /**
     * calculates the nearest center to each data point and adds the data to the cluster of respective center
     */
    private void assignToCluster() {
        ArrayList<Double> difference = new ArrayList<>();
        double dataItem, cenVal, diff;
        for(int i =0; i<data.size(); i++) {
            dataItem = data.get(i);
            for (int j=0; j<center.length; j++) {
                cenVal = center[j];
                diff = Math.abs(cenVal-dataItem);
                difference.add(diff);

            }
            int minIndex = getMinIndex(difference);
            clusterGroup[minIndex].add(dataItem);
            difference.removeAll(difference);
        }
    }

    /**
     * initialize the cluster centers to first k values
     */
    private void initializeCenters() {
        for (int i=0; i<this.k; i++) {
            center[i] = data.get(i);
            clusterGroup[i] = new ArrayList<Double>();
        }
    }

    /**
     * creates a group to hold data related to each cluster
     */
    private void createGroup() {
        for(int i=0; i<this.k; i++){
            this.clusterGroup.add(new ArrayList<>());
        }

    }

    /**
     * calculates distance between each data point and cluster and returns the closest center
     * @param diff: the distance to each cluster center for a data point
     * @return : the closest cluster
     */
    private int getMinIndex(ArrayList<Double> diff) {
        int minIndex = 0;
        for(int i = 1; i<diff.size(); i++){
            if(diff.get(minIndex)>diff.get(i)){
                minIndex = i;
            }

        }
        // System.out.println(diff);
        return minIndex;
    }

    /**
     * gives the cluster center the last data point belongs to
     * @return : center
     */
    public int getCenter(){
        for (int i = 0; i<clusterGroup.length; i++){
            for (int j=0; j<clusterGroup[i].size(); j++) {
                if(clusterGroup[i].get(j).equals(data.get((data.size()-1)))){

                    return i;

                }
            }
        }
        return -1;
    }




}
