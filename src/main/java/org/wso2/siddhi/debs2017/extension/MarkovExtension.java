package org.wso2.siddhi.debs2017.extension;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.debs2017.markov_chain.MarkovExecution;
import org.wso2.siddhi.query.api.definition.Attribute;

/**
 * Created by sachini on 2/3/17.
 */
public class MarkovExtension extends FunctionExecutor {

    MarkovExecution markovExecution = new MarkovExecution();
    @Override
    protected void init(ExpressionExecutor[] expressionExecutors, ExecutionPlanContext executionPlanContext) {

    }

    @Override
    protected Object execute(Object[] objects) {
        return null;
    }

    @Override
    protected Object execute(Object o) {
       // return markovExecution.execute((int)o, 8);
        return o;
    }

    @Override
    public Attribute.Type getReturnType() {
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