package org.wso2.siddhi.debs2017.input;

import org.hobbit.core.components.AbstractDataGenerator;

/**
 * Created by miyurud on 2/2/17.
 * @deprecated
 */
public class DataGenerator extends AbstractDataGenerator {
    @Override
    public void init() throws Exception {
        // Always init the super class first!
        super.init();

        // Your initialization code comes here...
    }

    @Override
    protected void generateData() throws Exception {
        // Create your data inside this method. You might want to use the
        // id of this data generator and the number of all data generators
        // running in parallel.

        int dataGeneratorId = getGeneratorId();
        int numberOfGenerators = getNumberOfGenerators();

        byte[] data = new byte[1];
        while(true) {
            // Create your data here
            //data = ...

            // the data can be sent to the task generator(s) ...
            sendDataToTaskGenerator(data);
            // ... and/or to the system
            sendDataToSystemAdapter(data);
        }
    }
}
