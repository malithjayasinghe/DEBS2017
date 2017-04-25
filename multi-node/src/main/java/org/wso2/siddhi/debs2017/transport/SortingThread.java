package org.wso2.siddhi.debs2017.transport;

import org.hobbit.core.data.RabbitQueue;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.debs2017.output.MultiNodeAlertGenerator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Sorts the events received from the Shiddhi worker nodes before publishing to rabbit mq output queue
 */
public class SortingThread extends Thread {
    static long timeout;
    private static ArrayList<Event> sortingList = new ArrayList<>();
    Event currentEvent;
    private static RabbitQueue rabbitMQPublisher;
    private static ArrayList<LinkedBlockingQueue<Event>> blockingQueues;
    int anomalycount = 0;
    private static int termination = 0;
    private static int queueNumebr;
    private static MultiNodeAlertGenerator multiNodeAlertGenerator;
    private static ArrayList<Integer> elements = new ArrayList<>();
    private static boolean sortByMachine;
    private static Anomaly anomaly;
    private static int currentTime = 0;
    private static int preTime;
    private static ArrayList<Anomaly> anomalyList = new ArrayList<>();


    /**
     * The constructor
     *
     * @param rmq            queue to publish the final output
     * @param blockingQueues link blocking queues to pull the data out
     */
    public SortingThread(RabbitQueue rmq, ArrayList<LinkedBlockingQueue<Event>> blockingQueues) {
        this.rabbitMQPublisher = rmq;
        this.blockingQueues = blockingQueues;
        //sortByMachine = sort;

        multiNodeAlertGenerator = new MultiNodeAlertGenerator(rmq);
        addQueues();
        for (int i = 0; i < 3; i++) {
            elements.add(0);
        }
    }

    /**
     * Start the sorting thread.
     */
    public void run() {
        sortByMachine = OutputServer.isSort;
        System.out.println(sortByMachine);
        while (true) {
            if (termination == 3) {
                if (anomalyList.size() != 0) {
                    publish(anomalyList);
                } else if (sortingList.size() != 0) {
                    for (int i = 0; i < sortingList.size(); i++) {
                        sort();
                    }
                }
                multiNodeAlertGenerator.terminate();
                System.exit(0);
                System.out.println("Termination received");
                break;
            } else {
                addFromQueues();
                //sort();
            }
        }

    }

    /**
     * Gets the timestamps of the sidhhi events
     *
     * @param event
     * @return the time stamp
     */

    private synchronized long getTime(Event event) {

        String abc = event.getData()[6].toString();
        long timestamp = Long.parseLong(abc);
        return timestamp;
    }

    /**
     * Get the machine number of the sidhhi events
     *
     * @param event
     * @return
     */
    private synchronized int getMachine(Event event) {
        String machine = event.getData()[0].toString();
        int machinenum = Integer.parseInt(machine.split("_")[1]);
        return machinenum;

    }


    /**
     * Get the property number of the sidhhi event
     */
    private synchronized int getPropery(Event event) {
        String property = event.getData()[2].toString();
        int propertynum = Integer.parseInt(property.split("_")[2]);
        return propertynum;
    }


    /**
     * get the event with the least timestamp and generate the alert
     */
    private void sort() {

        currentEvent = sortingList.get(0);
        for (int i = 0; i < sortingList.size(); i++) {
            if (currentEvent.getTimestamp() > sortingList.get(i).getTimestamp()) {
                currentEvent = sortingList.get(i);

            }
        }
        queueNumebr = Integer.parseInt(currentEvent.getData()[5].toString());
        elements.set(queueNumebr, 0);
        //  System.out.println(currentEvent + "sorted event");
        multiNodeAlertGenerator.generateAlert(currentEvent);
        sortingList.remove(currentEvent);

        anomalycount++;
        System.out.println(anomalycount + "Anomaly count");


    }

    /**
     * assign the linked blocking queues to the arraylist
     */
    public void addQueues() {
        for (int i = 0; i < 3; i++) {
            blockingQueues.add(new LinkedBlockingQueue<Event>());
        }
    }

    /**
     * retrieve the first event of each linked blocking queue and add to arraylist
     */


    public void addFromQueues() {
        for (int i = 0; i < elements.size(); i++) {
            try {
                if (elements.get(i) == 0) {
                    Event event = blockingQueues.get(i).take();
                    if (event.getTimestamp() != -1l) {
                        if (sortByMachine) {

                            anomaly = new Anomaly(Integer.parseInt(event.getData()[0].toString().split("_")[1]),
                                    Integer.parseInt(event.getData()[2].toString().split("_")[2]), event);

                            if (currentTime == 0) {
                                currentTime = Integer.parseInt(event.getData()[1].toString().split("_")[1]);
                                anomalyList.add(anomaly);
                            } else {
                                preTime = currentTime;
                                currentTime = Integer.parseInt(event.getData()[1].toString().split("_")[1]);
                                if (preTime == currentTime) {
                                    anomalyList.add(anomaly);
                                } else {
                                    publish(anomalyList);
                                    anomalyList.add(anomaly);
                                }
                            }
                        } else {
                            elements.set(i, -1);
                            sortingList.add(event);
                        }

                    } else {

                        elements.set(i, 2);
                        termination++;

                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (sortingList.size() != 0) {
            sort();
        }
    }

    public static void publish(ArrayList<Anomaly> anomalies) {
        Collections.sort(anomalyList);
        for (int i = 0; i < anomalies.size(); i++) {
            multiNodeAlertGenerator.generateAlert(anomalies.get(i).getEvent());
        }
        anomalies.clear();
    }


}
