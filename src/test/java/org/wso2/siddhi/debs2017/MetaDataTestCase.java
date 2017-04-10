package org.wso2.siddhi.debs2017;

/**
 * Created by sachini on 3/7/17.
 */

import org.apache.log4j.Logger;
import org.junit.Before;
import org.wso2.siddhi.debs2017.input.metadata.DebsMetaData;
import org.wso2.siddhi.debs2017.input.metadata.MetaDataItem;
import org.wso2.siddhi.debs2017.input.metadata.MetaDataQuery;


public class MetaDataTestCase {

    private static final Logger log = Logger.getLogger(MetaDataQuery.class);


    @Before
    public void init() {

    }

    @org.junit.Test
    public void Test1() throws InterruptedException {
        log.info("Meta data test case TestCase");
        MetaDataQuery.run("molding_machine_10M.metadata.nt");

        System.out.println(DebsMetaData.getMetaData().size());
        for(String key: DebsMetaData.getMetaData().keySet()){
            MetaDataItem dmm = DebsMetaData.getMetaData().get(key);

            System.out.println(dmm.getMachineNumber()+ " " + dmm.getDimension()+ " "+ dmm.getClusterCenters()+ " "
                    + dmm.getProbabilityThreshold() );
        }

        //System.out.println(DebsMetaData.meta.get("Machine_59"+"_59_104").getProbabilityThreshold());
    }

    @org.junit.Test
    public void Test2() throws InterruptedException {
        log.info("Meta data test case TestCase");
        DebsMetaData.generate("molding_machine_10M.metadata.nt");
        int count = 0;
        for(String key: DebsMetaData.getMetaData().keySet()){
            MetaDataItem dmm = DebsMetaData.getMetaData().get(key);
            count++;
            System.out.println(key + "Key");
            System.out.println(dmm.getMachineNumber()+ " " + dmm.getDimension()+ " "+ dmm.getClusterCenters()+ " "
                    + dmm.getProbabilityThreshold() );
        }
        System.out.println("Data items"+count);

    }



}
