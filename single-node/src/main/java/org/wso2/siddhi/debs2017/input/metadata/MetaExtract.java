package org.wso2.siddhi.debs2017.input.metadata;

import com.lmax.disruptor.RingBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.debs2017.input.sparql.RabbitMessage;
import org.wso2.siddhi.debs2017.input.sparql.RegexPattern;
import org.wso2.siddhi.debs2017.query.SingleNodeServer;
import org.wso2.siddhi.query.api.expression.condition.In;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;

/**
 * Created by sachini on 5/3/17.
 */
public class MetaExtract {


    private static final Logger logger = LoggerFactory.getLogger(MetaExtract.class);
    public static Map<String, MetaDataItem> meta = new ConcurrentHashMap<>(55000);
    private static InputStream is = null;
    private static BufferedReader bfReader = null;

    public static void load(String filePath) {

        Path path = Paths.get(filePath);
        //Path path = Paths.get("molding_machine_10M.metadata.nt");
        int count = 0;
        int nextOccurence = 6;


        int propCount = 5;
        int clusterCount = 7;
        int probCount = 9;

        String property = "";
        String clusters = "";

        Integer line = 0;

        try {
            byte[] data = Files.readAllBytes(path);

            is = new ByteArrayInputStream(data);
            bfReader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
            String temp = null;
            while ((temp = bfReader.readLine()) != null) {
                count++;

                if (count == propCount) {
                    property = temp;

                    propCount += nextOccurence;

                } else if (count == clusterCount) {
                    clusters = temp;
                    clusterCount += nextOccurence;



                }
                if (count == probCount) {

                    long sequence = SingleNodeServer.meta.next();
                    try {


                        MetaPattern metaPattern = SingleNodeServer.meta.get(sequence);

                        metaPattern.setProperty(property);
                        // System.out.println("property " + property);

                        metaPattern.setClusters(clusters);
                        metaPattern.setProbability(temp);
                        metaPattern.setLine(line);
                    }finally {
                        SingleNodeServer.meta.publish(sequence);
                    }

                    line = line +1;

                    probCount += nextOccurence;



                }
                if (count == 335) {

                    count = 1;
                    propCount = 5;
                    clusterCount = 7;
                    probCount = 9;
                }


            }
        } catch (UnsupportedEncodingException e) {
            logger.debug(e.getMessage());
        } catch (IOException e) {
            logger.debug(e.getMessage());
        } finally {
            try {
                if (is != null) {
                    is.close();
                }
            } catch (Exception ex) {
                logger.debug(ex.getMessage());
            }
        }


    }

    public static synchronized void addValue(String dimension, int clusterCenters,
                                 double probabilityThreshold) {
        MetaDataItem dm = new MetaDataItem(dimension, clusterCenters, probabilityThreshold);
        String mapKey = dm.getDimension();
        meta.put(mapKey, dm);
       //System.out.println(meta.size());
    }

    public static MetaDataItem getMetaDataItem(String dimension, int clusterCenters,
                                             double probabilityThreshold) {
        return new MetaDataItem(dimension, clusterCenters, probabilityThreshold);

        //System.out.println(meta.size());
    }

    public static Map<String, MetaDataItem> getMetaData() {
        return meta;
    }

}
