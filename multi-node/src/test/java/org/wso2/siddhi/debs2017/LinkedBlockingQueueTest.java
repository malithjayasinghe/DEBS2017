package org.wso2.siddhi.debs2017;

import org.junit.Assert;

import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by sachini on 3/31/17.
 */
public class LinkedBlockingQueueTest {


    private static LinkedBlockingQueue<Integer>[]  blockingQ= new LinkedBlockingQueue[3];



    static int[] data = {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,23,33,56,67,69,73,81};
    static ArrayList<Integer> arr = new ArrayList<>();
    static ArrayList<Integer> results = new ArrayList<>();
    static int sorted = 0;
    static long timeout ;
    static  int currentEvent;


    public static void insertdata(){
        for(int i =0; i<data.length;i++){
            if(data[i] % 3 == 0){
                try {
                    blockingQ[0].put(data[i]);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                }else if(data[i]%3 == 1){
                try {
                    blockingQ[1].put(data[i]);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else if(data[i]%3 == 2){
                try {
                    blockingQ[2].put(data[i]);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    public static void insert()  {
        while (true){
        for(int i=0; i <blockingQ.length; i++) {

            /*if (blockingQueues[i].peek() != null) {
                sortingList.add(blockingQueues[i].peek());
            } else {*/
            timeout = System.currentTimeMillis();
            while (true) {
                if ((System.currentTimeMillis() - timeout) >= 2) {
                    //  queue1 = true;
                    break;
                } else if (blockingQ[i].peek() != null) {
                    arr.add(blockingQ[i].peek());
                    break;
                }
            }
        }

        sort();
        if(sorted == data.length) {
            checkResults();
            break;
        }


        }

        }

    public static void addQueues(){
        for(int i=0; i <blockingQ.length; i++){
            blockingQ[i] = new LinkedBlockingQueue<Integer>();
        }
    }



    public static void sort(){
        if (arr.size() >= 1) {
            currentEvent = arr.get(0);
            for (int i = 1; i < arr.size(); i++) {
                if (currentEvent > arr.get(i))
                    currentEvent = arr.get(i);
            }
            arr.clear();
            System.out.println(currentEvent);
            results.add(currentEvent);
            sorted++;
            removeEvent(currentEvent);
        }
    }

    public static void removeEvent(int event){
        if(event%3 == 0)
            blockingQ[0].poll();
        else if(event%3 == 1)
            blockingQ[1].poll();
        else
            blockingQ[2].poll();
    }

    public static void checkResults(){
        for(int i =0; i<data.length; i++){
           Assert.assertEquals(data[i],(int)results.get(i));
        }
    }


    @org.junit.Test
    public void Test1 () {

        addQueues();
        insertdata();
        insert();

    }




}
