package org.wso2.siddhi.debs2017;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.hobbit.core.components.Component;

/**
 * Created by miyurud on 2/2/17.
 * @deprecated
 */
public class ComponentStarter {
    private static final int ERROR_EXIT_CODE = -1;
    public static void main(String[] args) {
        Logger logger = Logger.getLogger(ComponentStarter.class);

        Component component = null;
        boolean success = true;
        try {
            //component = createComponentInstance(args[0]);
            // initialize the component
            component.init();
            // run the component
            logger.info("----AAAA");
            component.run();
            logger.info("----BBBB");
        } catch (Throwable t) {
            logger.error("Exception while executing component. Exiting with error code.", t);
            success = false;
        } finally {
            IOUtils.closeQuietly(component);
        }

        if (!success) {
            System.exit(ERROR_EXIT_CODE);
        }
    }
}
