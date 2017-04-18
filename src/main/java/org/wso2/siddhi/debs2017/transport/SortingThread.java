package org.wso2.siddhi.debs2017.transport;

import org.hobbit.core.data.RabbitQueue;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.debs2017.output.MultiNodeAlertGenerator;
import org.wso2.siddhi.debs2017.output.RabbitMQPublisher;
import org.wso2.siddhi.query.api.expression.condition.In;

import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Sorts the events received from the Shiddhi worker nodes before publishing to rabbit mq output queue
 */
public class SortingThread extends Thread {
    static long timeout;
    private static ArrayList<Event> sortingList = new ArrayList<>();
    Event currentEvent;
    private static RabbitQueue rabbitMQPublisher;
    private static LinkedBlockingQueue<Event>[] blockingQueues;
    int anomalycount = 0;
    private static int termination = 0;

    /**
     * The constructor
     *
     * @param rmq queue to publish the final output
     * @param blockingQueues link blocking queues to pull the data out
     */
    public SortingThread(RabbitQueue rmq, LinkedBlockingQueue<Event>[] blockingQueues) {
        this.rabbitMQPublisher = rmq;
        this.blockingQueues = blockingQueues;
        addQueues();
    }

    /**
     * Start the sorting thread.
     *
     */
    public void run() {
        while (true) {
            checkQueue();
            sort();
        }


    }

    /**
     * Gets the timestamps of the sidhhi events
     *
     * @param event
     * @return the time stamp
     */

    private synchronized int getTime(Event event) {
        String time = (String) event.getData()[1];
        int timestamp = Integer.parseInt(time.substring(10));
        return timestamp;
    }

    /**
     * Get the machine number of the sidhhi events
     * @param event
     * @return
     */
    private synchronized int getMachine(Event event){
        String machine = event.getData()[0].toString();
        int machinenum = Integer.parseInt(machine.split("_")[1]);
        return machinenum;

    }


    /**
     * Get the property number of the sidhhi event
     */
    private synchronized int getPropery(Event event){
        String property = event.getData()[2].toString();
        int propertynum = Integer.parseInt(property.split("_")[2]);
        return propertynum;
    }

    /**
     * remove the event from the corresponding queue
     *
     * @param e the event to be removed
     */

    private void removeEvent(Event e) {
        int n = (Integer) e.getData()[5];
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
     * assign the linked blocking queues to the array
     */
    public void addQueues() {
        for (int i = 0; i < blockingQueues.length; i++) {
            blockingQueues[i] = new LinkedBlockingQueue<Event>();
        }
    }

    /**
     * retrieve the first event of each linked blocking queue and add to arraylist
     */

    public void checkQueue() {
        for (int i = 0; i < blockingQueues.length; i++) {
            timeout = System.currentTimeMillis();
            while (true) {
                if ((System.currentTimeMillis() - timeout) >= 2) {
                    break;
                } else if (blockingQueues[i].peek() != null) {
                    sortingList.add(blockingQueues[i].peek());
                    break;
                }

            }
        }
    }
}
