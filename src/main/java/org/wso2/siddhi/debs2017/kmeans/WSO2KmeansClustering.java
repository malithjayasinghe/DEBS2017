package org.wso2.siddhi.debs2017.kmeans;

import java.util.ArrayList;

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
    private ArrayList<ArrayList<Double>> clusterGroup;
    private ArrayList<Double> center;
    private ArrayList<Double> centerOld;

    public WSO2KmeansClustering(int c, int max, ArrayList<Double> dataSet ){

        //intialize variables
        this.k = c;
        this.maxIter = max;
        this.data = dataSet;
        this.clusterGroup = new ArrayList<>();
        this.center = new ArrayList<>();
        this.centerOld = new ArrayList<>();

        //create a group to hold data related to each cluster
        createGroup();
        //System.out.println("No of Clusters :"+this.k+"\nNo of clustergroups :"+this.clusterGroup.size());

        //initialize the cluster centers to first k values
        initializeCenters();
        /*System.out.println("\nInitial center values");
        for (int i=0; i<center.size(); i++){
            System.out.println("c"+i+":"+center.get(i));
        }*/

        int iter = 0;
        do {
            //calculate the nearest center to each data point and add the data to the cluster of respective center
            assignToCluster();
            /*System.out.println("\nChecking each cluster");
            for (int i=0; i<clusterGroup.size(); i++){
                System.out.println("clusterG "+i+":"+clusterGroup.get(i));
            };

            for (int i = 0; i < center.size(); i++) {
                System.out.println("New C" + (i) + " " + center.get(i));
            }*/


            //reinitialize the cluster centres and store the old ones
            reintializeCluster(iter);

            if (!center.equals(centerOld)) {
                for (int i = 0; i < clusterGroup.size(); i++) {
                    clusterGroup.get(i).removeAll(clusterGroup.get(i));
                }
            }
            iter++;
        } while (!center.equals(centerOld) && iter<=maxIter);

        //System.out.println("Iterations : "+iter);


    }

    private void reintializeCluster(int iter) {
        for (int i = 0; i < k; i++) {
            if (iter == 0) {
                centerOld.add(center.get(i));
            } else {
                centerOld.set(i, center.get(i));
            }
            if (!clusterGroup.get(i).isEmpty()) {
                center.set(i, average(clusterGroup.get(i)));
            }
        }
    }

    private Double average(ArrayList<Double> doubles) {
        double sum = 0;
        for (int i =0; i<doubles.size(); i++) {
            sum = sum +doubles.get(i);
        }
        return (sum/doubles.size());
    }

    private void assignToCluster() {
        ArrayList<Double> difference = new ArrayList<>();
        double dataItem, cenVal, diff;
        for(int i =0; i<data.size(); i++) {
            dataItem = data.get(i);
            for (int j=0; j<center.size(); j++) {
                cenVal = center.get(j);
                diff = Math.abs(cenVal-dataItem);
                difference.add(diff);

            }
            int minIndex = getMinIndex(difference);
            clusterGroup.get(minIndex).add(dataItem);
            difference.removeAll(difference);
        }
    }

    private void initializeCenters() {
        for (int i=0; i<this.k; i++) {
            center.add(data.get(i));
        }
    }

    private void createGroup() {
        for(int i=0; i<this.k; i++){
            this.clusterGroup.add(new ArrayList<>());
        }

    }


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

    public int getCenter(){
        for (int i = 0; i<clusterGroup.size(); i++){
            for (int j=0; j<clusterGroup.get(i).size(); j++) {
                if(clusterGroup.get(i).get(j).equals(data.get((data.size()-1)))){

                    return i;

                }
            }
        }
        return -1;
    }




}
