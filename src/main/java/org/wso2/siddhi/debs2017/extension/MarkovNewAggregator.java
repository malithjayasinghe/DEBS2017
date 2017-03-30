package org.wso2.siddhi.debs2017.extension;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.selector.attribute.aggregator.AttributeAggregator;
import org.wso2.siddhi.debs2017.markovchain.Markovnew;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.ArrayList;

/**
 * Created by sachini on 3/29/17.
 */
public class MarkovNewAggregator extends AttributeAggregator {
    private boolean windowfull = false;
    Markovnew markovnew = new Markovnew();
    private ArrayList<Integer> centers;
    private  double probability ;
    @Override
    protected void init(ExpressionExecutor[] expressionExecutors, ExecutionPlanContext executionPlanContext) {

    }

    @Override
    public Attribute.Type getReturnType() {
        return null;
    }

    @Override
    public Object processAdd(Object o) {
        centers = (ArrayList<Integer>)o;
        if(windowfull == true){
            markovnew.execute(centers);

            //get the total probability
            probability =  markovnew.updateProbability(centers);
            System.out.println("Prob"+ probability);


        }
        return probability;
    }

    @Override
    public Object processAdd(Object[] objects) {
        return null;
    }

    @Override
    public Object processRemove(Object o) {
        windowfull = true;

        return null;
    }

    @Override
    public Object processRemove(Object[] objects) {
        return null;
    }

    @Override
    public Object reset() {
        return null;
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public Object[] currentState() {
        return new Object[0];
    }

    @Override
    public void restoreState(Object[] objects) {

    }
}
