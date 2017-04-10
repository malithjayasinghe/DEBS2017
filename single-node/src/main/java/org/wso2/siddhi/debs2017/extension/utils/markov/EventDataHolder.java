package org.wso2.siddhi.debs2017.extension.utils.markov;

/**
 * Holder for event data
 */
public class EventDataHolder {

    private int eventCount = 0;

    public int getEventCount() {
        return eventCount;
    }

    public void setEventCount(int eventCount) {
        this.eventCount = eventCount;
    }

    public double getProbability() {
        return probability;
    }

    public void setProbability(double probability) {
        this.probability = probability;
    }

    private double probability = 1.0;

}
