package org.wso2.siddhi.debs2017;

/**
 * Created by sachini on 3/7/17.
 */

import org.apache.log4j.Logger;
import org.junit.Before;
import org.wso2.siddhi.debs2017.input.DebsMetaData;
import  org.wso2.siddhi.debs2017.input.MetaDataQuery;


public class MetaDataTest {

    private static final Logger log = Logger.getLogger(MetaDataQuery.class);


    @Before
    public void init() {

    }

    @org.junit.Test
    public void Test1() throws InterruptedException {
        log.info("Meta data test case TestCase");
        MetaDataQuery.run();

        System.out.println(DebsMetaData.meta.size());
        for(String key: DebsMetaData.meta.keySet()){
            DebsMetaData dmm = DebsMetaData.meta.get(key);
            System.out.println(dmm.getMachineNumebr()+ " " + dmm.getDimension()+ " " + dmm.getModel()+ " "+ dmm.getClusterCenters()+ " "
                    + dmm.getProbabilityThreshold() + " " + dmm.getWindowLength() );
        }
    }

}
