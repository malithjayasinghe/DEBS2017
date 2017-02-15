package org.wso2.siddhi.debs2017;

import org.wso2.siddhi.debs2017.input.UnixConverter;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;

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
public class UnixTimeTestCase {

    public static void main(String[] args) {
        BufferedReader reader = null;
        BufferedReader reader1 = null;

        try {
            reader = new BufferedReader(new FileReader("rdfData_extract_100m_time.csv"));

            String line = null;
            Scanner scanner = null;

            int index = 0;
            while ((line = reader.readLine()) != null) {

                scanner = new Scanner(line);
                scanner.useDelimiter(",");

                while (scanner.hasNext()) {
                    index++;
                    String data = scanner.next();
                    if(index==3){



                        System.out.println(data+"\t"+ UnixConverter.getUnixTime(data));
                    }





                }
                index =0;

            }

            //close reader
            reader.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
