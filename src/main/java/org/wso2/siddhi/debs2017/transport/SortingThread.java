package org.wso2.siddhi.debs2017.transport;

import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.debs2017.transport.test.TestListener;

import java.util.ArrayList;

/**
 * Created by sachini on 3/31/17.
 */
public class SortingThread extends Thread {
    static long timeOut0 = 0;
    static long timeout1 = 0;
    static long timeout2 = 0;
    static long timeout;
    static int count = 0;
    private static ArrayList<Event> sortingList = new ArrayList<>();
    Event currentEvent;

    public void run() {
        while (true) {
            if (TestListener.lbqueue0.peek() != null) {
                sortingList.add(TestListener.lbqueue0.peek());
            } else {
                timeout = System.currentTimeMillis();
               while(true){
                  // System.out.println("Inner while loop 1");
                   if((System.currentTimeMillis() -  timeout)>=2){
                      break;
                  }else if(TestListener.lbqueue0.peek() != null)
                   break;
               }
            }
            if (TestListener.lbqueue1.peek() != null) {
                sortingList.add(TestListener.lbqueue1.peek());
            } else {
                timeout = System.currentTimeMillis();
                while(true){
                   // System.out.println("inner while loop 2");
                    if((System.currentTimeMillis() -  timeout)>=2){
                        break;

                    }else if(TestListener.lbqueue0.peek() != null)
                        break;
                }
            }

            if (TestListener.lbqueue2.peek() != null) {
                sortingList.add(TestListener.lbqueue2.peek());
            } else {
                timeout = System.currentTimeMillis();
                while(true){
                   // System.out.println("inner while loop 3");
                    if((System.currentTimeMillis() -  timeout)>=2){
                        break;
                    }else if(TestListener.lbqueue0.peek() != null)
                        break;
                }
            }


                sort();
          //  System.out.println("Inner while loop");


        }


    }

    private synchronized int getTime(Event event) {
        String time = (String) event.getData()[1];
        int timestamp = Integer.parseInt(time.substring(10));
        return timestamp;
    }


    private void removeEvent(Event e) {
        int n = (Integer)e.getData()[6];
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
            System.out.println(currentEvent);
            count++;
            System.out.println(count);
            removeEvent(currentEvent);
        }
    }
}
