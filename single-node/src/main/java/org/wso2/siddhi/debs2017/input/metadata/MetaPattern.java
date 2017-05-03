package org.wso2.siddhi.debs2017.input.metadata;

import org.wso2.siddhi.query.api.expression.condition.In;

import java.util.regex.Pattern;

/**
 * Created by sachini on 5/3/17.
 */
public class MetaPattern {


    String property;
    String probability;
    String clusters;
    Integer line;

    public int getLine() {
        return line;
    }

    public void setLine(int line) {
        this.line = line;
    }



    public String getProperty() {
        return property;
    }

    public void setProperty(String property) {
        this.property = property;
    }

    public String getProbability() {
        return probability;
    }

    public void setProbability(String probability) {
        this.probability = probability;
    }

    public String getClusters() {
        return clusters;
    }

    public void setClusters(String clusters) {
        this.clusters = clusters;
    }



}
