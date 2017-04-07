package org.wso2.siddhi.debs2017.transport;

import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.debs2017.output.MultiNodeAlertGenerator;
import org.wso2.siddhi.debs2017.output.RabbitMQPublisher;


import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;


public class SortingThread extends Thread {
    static long timeout;
    private static ArrayList<Event> sortingList = new ArrayList<>();
    Event currentEvent;
    private static RabbitMQPublisher rabbitMQPublisher;
    private  static LinkedBlockingQueue<Event>[] blockingQueues;
    int anomalycount=0;

    public SortingThread(RabbitMQPublisher rmq,LinkedBlockingQueue<Event>[] blockingQueues){
        this.rabbitMQPublisher = rmq;
        this.blockingQueues = blockingQueues;
        addQueues();
    }



    public void run() {
        while (true) {
            checkQueue();
            sort();
        }

    }

    /**
     * compare the timestamps of the sidhhi events
     * @param event
     * @return
     */

    private synchronized int getTime(Event event) {
        String time = (String) event.getData()[1];
        int timestamp = Integer.parseInt(time.substring(10));
        return timestamp;
    }

    /**
     * remove the event with the leas ttimestamp from therespective queue
     * @param e
     */

    private void removeEvent(Event e) {
        int n = (Integer)e.getData()[5];
       // System.out.println(n +"removed index");
        if (n == 0) {
           blockingQueues[0].poll();
        } else if (n == 1) {
            blockingQueues[1].poll();
        } else {
            blockingQueues[2].poll();
        }
    }

    /**
     * get the event with the least timestamp and generate the alert
     */
    private void sort() {

        if (sortingList.size() >= 1) {
            currentEvent = sortingList.get(0);
            for (int i = 1; i < sortingList.size(); i++) {
                if (getTime(currentEvent) > getTime(sortingList.get(i)))
                    currentEvent = sortingList.get(i);
            }
            sortingList.clear();
            MultiNodeAlertGenerator ag = new MultiNodeAlertGenerator(currentEvent, rabbitMQPublisher);
            ag.generateAlert();
            anomalycount++;
            System.out.println(anomalycount);
            removeEvent(currentEvent);
        }
    }

    /**
     * assign the linkedblocking queues to the array
     */
    public void addQueues(){
       for(int i=0; i <blockingQueues.length; i++){
           blockingQueues[i] = new LinkedBlockingQueue<Event>();
       }
    }

    /**
     * retrieve the first event of each linkedblockingqueue and add to arraylist
     */

    public void checkQueue(){
        for(int i=0; i <blockingQueues.length; i++){
            /*if (blockingQueues[i].peek() != null) {
                sortingList.add(blockingQueues[i].peek());
            } else {*/
                timeout = System.currentTimeMillis();
                while (true) {
                    if ((System.currentTimeMillis() - timeout) >= 2) {
                        //  queue1 = true;
                        break;
                    } else if (blockingQueues[i].peek() != null) {
                        sortingList.add(blockingQueues[i].peek());
                        break;
                    }
                }
           // }
        }
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
