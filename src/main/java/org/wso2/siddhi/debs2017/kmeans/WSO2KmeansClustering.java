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

//NOT USED
public class WSO2KmeansClustering {



    private int k;
    private int maxIter;
    private ArrayList<Double> data = new ArrayList<>();
    private ArrayList<Double>[] clusterGroup;
    private double[] center;
    private double[] centerOld;
    private ArrayList<Double>  kDist;


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

        if(k > data.size()){
            this.k = data.size();
        }

        //get the first k distinct values in the data
        int count = 0;
        kDist = new ArrayList<>();
        for (int i =0; i<data.size(); i++){
            if (! kDist.contains(data.get(i))){
                kDist.add(data.get(i));
                count++;
            }
            if (count==k){
                break;
            }
        }
        // System.out.println("kDist : "+kDist);

        // if k values are not found, k should be the same as distinct
        if(kDist.size()<k){
            k = kDist.size();
        }


        this.clusterGroup = new ArrayList[k];
        this.center = new double[k];
        this.centerOld =  new double[k];


        createGroup();
        //System.out.println("No of Clusters :"+this.k+"\nNo of clustergroups :"+this.clusterGroup.length);

        initializeCenters();
        /*System.out.println("\nInitial center values");
        for (int i=0; i<center.length; i++){
            System.out.println("c"+i+":"+center[i]);
        }*/

        int iter = 0;
        do {

            assignToCluster();
            /*System.out.println("\nChecking each cluster");
            for (int i=0; i<clusterGroup.length; i++){
                System.out.println("clusterG "+i+":"+clusterGroup[i]);
            };

            for (int i = 0; i < center.length; i++) {
                System.out.println("New C" + (i) + " " + center[i]);
            }*/

            reinitializeCluster(iter);


            if (!Arrays.equals(center, centerOld)) {
                for (int i = 0; i < clusterGroup.length; i++) {
                    clusterGroup[i].removeAll(clusterGroup[i]);

                }
            }
            iter++;

        } while (!Arrays.equals(center, centerOld) && iter<maxIter);

        // System.out.println("Iterations : "+iter);


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
            //difference.removeAll(difference);
            difference = new ArrayList<>();
        }
    }

    /**
     * initialize the cluster centers to first k values
     */
    private void initializeCenters() {
        for (int i=0; i<this.k; i++) {
            center[i] = kDist.get(i);
            clusterGroup[i] = new ArrayList<>();
        }
    }

    /**
     * creates a group to hold data related to each cluster
     */
    private void createGroup() {
        for(int i=0; i<this.k; i++){
            this.clusterGroup[i] = new ArrayList<>();
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
    public int getCenter(double value){
        ArrayList<Double> minDist = new ArrayList<>();
        //take the copy of original
        double [] unsoted = new double[center.length];
        for(int i =0; i<unsoted.length; i++){
            unsoted[i] = center[i];
        }

        //sort the array
        Arrays.sort(center);

        //get distance to each
        for(int i =0; i<center.length; i++){
            minDist.add(Math.abs(center[i]-value));

        }

        return getMinIndex(minDist)+1;
    }

    /*public static void main(String[] args) {
        ArrayList<Double> input = new ArrayList<>();
        input.add(1.0);
        input.add(3.0);
        input.add(2.0);
        input.add(4.0);
        input.add(5.0);

        WSO2KmeansClustering test =new WSO2KmeansClustering(2, 10, input);
        System.out.println(test.getEventCount(1.0));
    }*/






}
