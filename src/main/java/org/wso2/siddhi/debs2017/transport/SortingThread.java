package org.wso2.siddhi.debs2017.transport;

import javafx.scene.control.Alert;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.debs2017.Output.AlertGeneratMultiNode;
import org.wso2.siddhi.debs2017.Output.RabbitMQPublisher;
import org.wso2.siddhi.debs2017.transport.test.TestListener;

import java.util.ArrayList;


public class SortingThread extends Thread {
    static  boolean queue1 = false;
    static boolean queue2 = false;
    static boolean queue3 = false;

    static long timeout;
    static int count = 0;
    private static ArrayList<Event> sortingList = new ArrayList<>();
    Event currentEvent;
    private static RabbitMQPublisher rabbitMQPublisher;

    public void run() {
        while (true) {
           // if(queue1 == false) {
                if (TestListener.lbqueue0.peek() != null) {
                    sortingList.add(TestListener.lbqueue0.peek());
                } else {
                    timeout = System.currentTimeMillis();
                    while (true) {
                        // System.out.println("Inner while loop 1");
                        if ((System.currentTimeMillis() - timeout) >= 2) {
                          //  queue1 = true;
                            break;
                        } else if (TestListener.lbqueue0.peek() != null) {
                            sortingList.add(TestListener.lbqueue0.peek());
                            break;
                        }
                    }
                }
           // }
          //  if(queue2 == false) {
                if (TestListener.lbqueue1.peek() != null) {
                    sortingList.add(TestListener.lbqueue1.peek());
                } else {
                    timeout = System.currentTimeMillis();
                    while (true) {
                        // System.out.println("inner while loop 2");
                        if ((System.currentTimeMillis() - timeout) >= 2) {
                          //  queue2 = true;
                            break;
                        } else if (TestListener.lbqueue1.peek() != null) {
                            sortingList.add(TestListener.lbqueue1.peek());
                            break;
                        }
                    }
                }
          //  }

           // if(queue3 == false) {
                if (TestListener.lbqueue2.peek() != null) {
                    sortingList.add(TestListener.lbqueue2.peek());
                } else {
                    timeout = System.currentTimeMillis();
                    while (true) {
                        // System.out.println("inner while loop 3");
                        if ((System.currentTimeMillis() - timeout) >= 2) {
                            break;
                        } else if (TestListener.lbqueue2.peek() != null) {
                            sortingList.add(TestListener.lbqueue2.peek());
                            break;
                        }
                    }
                }
          //  }

            sort();
        }
    }

    private synchronized int getTime(Event event) {
        String time = (String) event.getData()[1];
        int timestamp = Integer.parseInt(time.substring(10));
        return timestamp;
    }


    private void removeEvent(Event e) {
        int n = (Integer)e.getData()[5];
       // System.out.println(n +"removed index");
        if (n == 0) {
            TestListener.lbqueue0.poll();
        } else if (n == 1) {
            TestListener.lbqueue1.poll();
        } else {
            TestListener.lbqueue2.poll();
        }
    }

    private void sort() {
        //System.out.println(sortingList.size() + "Arraylist size");
        if (sortingList.size() >= 1) {
            currentEvent = sortingList.get(0);
            for (int i = 1; i < sortingList.size(); i++) {
                if (getTime(currentEvent) > getTime(sortingList.get(i)))
                    currentEvent = sortingList.get(i);
            }
            sortingList.clear();
            AlertGeneratMultiNode ag = new AlertGeneratMultiNode(currentEvent, rabbitMQPublisher);
            ag.generateAlert();
           // System.out.println(currentEvent);
            //count++;
            //System.out.println(count);
            removeEvent(currentEvent);
        }
    }

    public SortingThread(RabbitMQPublisher rmq){
        this.rabbitMQPublisher = rmq;
    }

   /* private void sortTerminate(){
        while (true){
            while (true){
                if(queue1 == true){
                    break;
                }else if(TestListener.lbqueue0.peek()!=null){
                    sortingList.add(TestListener.lbqueue0.peek());
                    break;
                }else if(TestListener.lbqueue0.peek() =="terminated"){
                    queue1 = true;
                    break;
                }

            }
            while (true){
                if(queue2 == true){
                    break;
                }else if(TestListener.lbqueue1.peek()!=null){
                    sortingList.add(TestListener.lbqueue1.peek());
                    break;
                }else if(TestListener.lbqueue1.peek() =="terminated"){
                    queue2 = true;
                    break;
                }

            }
            while (true){
                if(queue3 == true){
                    break;
                }else if(TestListener.lbqueue2.peek()!=null){
                    sortingList.add(TestListener.lbqueue1.peek());
                    break;
                }else if(TestListener.lbqueue2.peek() =="terminated"){
                    queue3 = true;
                    break;
                }

            }
            sort();
        }
    }*/







}
