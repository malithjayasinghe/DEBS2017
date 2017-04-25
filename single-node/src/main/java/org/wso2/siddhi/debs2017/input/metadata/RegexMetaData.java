package org.wso2.siddhi.debs2017.input.metadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/*
* Copyright (c) 2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

/**
 * RegexMetaData
 */
public class RegexMetaData {

    private static final Logger logger = LoggerFactory.getLogger(RegexMetaData.class);
    private static Pattern patternPropety = Pattern.compile("rty>.<http://www.agtinternational." +
            "com/ontologies/WeidmullerMetadata#(\\w*)>");
    private static Pattern patternProbability = Pattern.compile("e#valueLiteral>.\"(.*)\"");
    private static Pattern patternClusters = Pattern.compile("mberOfClusters>.\"(.*)\"");

    private static InputStream is = null;
    private static BufferedReader bfReader = null;

    private static HashMap<String, MetaDataItem> meta = new HashMap<>(55000);


    public static void load(String filePath) {

        Path path = Paths.get(filePath);
        //Path path = Paths.get("molding_machine_10M.metadata.nt");
        int count = 0;
        int nextOccurence = 6;


        int propCount = 5;
        int clusterCount = 7;
        int probCount = 9;

        String property = "";
        int clusters = 0;
        double probabilty = 0.0;

        try {
            byte[] data = Files.readAllBytes(path);

            is = new ByteArrayInputStream(data);
            bfReader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
            String temp = null;
            while ((temp = bfReader.readLine()) != null) {
                count++;

                if (count == propCount) {
                    propCount += nextOccurence;

                    Matcher matcheProp = patternPropety.matcher(temp);
                    if (matcheProp.find()) {
                        property = matcheProp.group(1);
                    }

                } else if (count == clusterCount) {
                    clusterCount += nextOccurence;

                    Matcher matcher = patternClusters.matcher(temp);
                    if (matcher.find()) {
                        clusters = Integer.parseInt(matcher.group(1));
                    }

                }
                if (count == probCount) {
                    probCount += nextOccurence;

                    Matcher matcher = patternProbability.matcher(temp);
                    if (matcher.find()) {
                        probabilty = Double.parseDouble(matcher.group(1));
                        addValue(property, clusters, probabilty);

                    }

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

    private static void addValue(String dimension, int clusterCenters,
                                 double probabilityThreshold) {
        MetaDataItem dm = new MetaDataItem(dimension, clusterCenters, probabilityThreshold);
        String mapKey = dm.getDimension();
        meta.put(mapKey, dm);
    }

    public static HashMap<String, MetaDataItem> getMetaData() {
        return meta;
    }


    public static void generate(String filePath, int machines) {

        Path path = Paths.get(filePath);

        int count = 0;
        int nextOccurence = 6;


        int propCount = 5;
        int clusterCount = 7;
        int probCount = 9;

        String property = "";
        int clusters = 0;
        double probabilty = 0.0;

        try {
            byte[] data = Files.readAllBytes(path);

            is = new ByteArrayInputStream(data);
            bfReader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
            String temp;

            while ((temp = bfReader.readLine()) != null) {
                count++;


                if (count == propCount) {
                    propCount += nextOccurence;

                    Matcher matcheProp = patternPropety.matcher(temp);
                    if (matcheProp.find()) {
                        property = matcheProp.group(1);
                    }

                } else if (count == clusterCount) {
                    clusterCount += nextOccurence;

                    Matcher matcher = patternClusters.matcher(temp);
                    if (matcher.find()) {
                        clusters = Integer.parseInt(matcher.group(1));
                    }

                }
                if (count == probCount) {
                    probCount += nextOccurence;

                    Matcher matcher = patternProbability.matcher(temp);
                    if (matcher.find()) {
                        probabilty = Double.parseDouble(matcher.group(1));
                        for (int i = 0; i < machines; i++) {
                            property = property.replace("_59_", "_" + i + "_");
                            addValue(property, clusters, probabilty);
                        }

                    }

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
}
