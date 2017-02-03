package org.wso2.siddhi.debs2017;

import org.apache.jena.rdf.model.NodeIterator;
import org.hobbit.core.Commands;
import org.hobbit.core.components.AbstractBenchmarkController;

import java.io.IOException;

/**
 * Created by miyurud on 2/2/17.
 */
public class BenchmarkController extends AbstractBenchmarkController {
    @Override
    public void init() throws Exception {
        super.init();

        // Your initialization code comes here...

        // You might want to load parameters from the benchmarks parameter model
        NodeIterator iterator = benchmarkParamModel.listObjectsOfProperty(benchmarkParamModel
                .getProperty("http://example.org/myParameter"));

        // Create the other components

        // Create data generators
        String dataGeneratorImageName = "example-data-generator";
        int numberOfDataGenerators = 1;
        String[] envVariables = new String[]{"key1=value1", "key2=value2"};
        createDataGenerators(dataGeneratorImageName, numberOfDataGenerators, envVariables);

        // Create task generators
        String taskGeneratorImageName = "example-task-generator";
        int numberOfTaskGenerators = 1;
        envVariables = new String[]{"key1=value1", "key2=value2"};
        createTaskGenerators(taskGeneratorImageName, numberOfTaskGenerators, envVariables);

        // Create evaluation storage
        createEvaluationStorage();

        // Wait for all components to finish their initialization
        //waitForComponents();
    }

    @Override
    protected void executeBenchmark() throws Exception {
        // give the start signals
        sendToCmdQueue(Commands.TASK_GENERATOR_START_SIGNAL);
        sendToCmdQueue(Commands.DATA_GENERATOR_START_SIGNAL);

        // wait for the data generators to finish their work
        waitForDataGenToFinish();

        // wait for the task generators to finish their work
        waitForTaskGenToFinish();

        // wait for the system to terminate
        waitForSystemToFinish();

        // Create the evaluation module
        String evalModuleImageName = "example-eval-module";
        String[] envVariables = new String[]{"key1=value1", "key2=value2"};
        createEvaluationModule(evalModuleImageName, envVariables);

        // wait for the evaluation to finish
        waitForEvalComponentsToFinish();

        // the evaluation module should have sent an RDF model containing the
        // results. We should add the configuration of the benchmark to this
        // model.
        // this.resultModel.add(...);

        // Send the resultModul to the platform controller and terminate
        sendResultModel(resultModel);
    }

    @Override
    public void close() throws IOException {
        // Free the resources you requested here

        // Always close the super class after yours!
        super.close();
    }
}
