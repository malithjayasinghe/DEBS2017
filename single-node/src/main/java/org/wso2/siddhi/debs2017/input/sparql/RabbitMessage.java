package org.wso2.siddhi.debs2017.input.sparql;

import org.wso2.siddhi.core.event.Event;

import java.util.regex.Pattern;

/**
 * Created by sachini on 4/20/17.
 */
public class RabbitMessage {
    private long time;
    private String timestamp;
    private String machine;
    private String property;
    private String value;
    private int line;
    private Event event;
    private long applicationTime;

    public boolean isTerminated() {
        return terminated;
    }

    public void setTerminated(boolean terminated) {
        this.terminated = terminated;
    }

    private boolean terminated;

    public boolean isStateful() {
        return stateful;
    }

    public void setStateful(boolean stateful) {
        this.stateful = stateful;
    }

    private boolean stateful;

    public long getApplicationTime() {
        return applicationTime;
    }

    public void setApplicationTime(long applicationTime) {
        this.applicationTime = applicationTime;
    }



    public Event getEvent() {
        return event;
    }

    public void setEvent(Event event) {
        this.event = event;
    }



    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getMachine() {
        return machine;
    }

    public void setMachine(String machine) {
        this.machine = machine;
    }

    public String getProperty() {
        return property;
    }

    public void setProperty(String property) {
        this.property = property;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }







    public int getLine() {
        return line;
    }

    public void setLine(int line) {
        this.line = line;
    }






}
