package org.wso2.siddhi.debs2017;

/**
 * Created by sachini on 3/7/17.
 */

import org.apache.log4j.Logger;
import org.junit.Before;
import org.wso2.siddhi.debs2017.input.metadata.DebsMetaData;
import org.wso2.siddhi.debs2017.input.metadata.MetaDataQuery;


public class MetaDataTest {

    private static final Logger log = Logger.getLogger(MetaDataQuery.class);


    @Before
    public void init() {

    }

    @org.junit.Test
    public void Test1() throws InterruptedException {
        log.info("Meta data test case TestCase");
        MetaDataQuery.run("molding_machine_1M.metadata.nt");

        System.out.println(DebsMetaData.meta.size());
        for(String key: DebsMetaData.meta.keySet()){
            DebsMetaData dmm = DebsMetaData.meta.get(key);
            System.out.println(dmm.getMachineNumebr()+ " " + dmm.getDimension()+ " "+ dmm.getClusterCenters()+ " "
                    + dmm.getProbabilityThreshold() );
        }

        System.out.println(DebsMetaData.meta.get("Machine_59"+"_59_5").getProbabilityThreshold());
    }



}
