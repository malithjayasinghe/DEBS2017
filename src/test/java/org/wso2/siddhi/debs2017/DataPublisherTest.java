package org.wso2.siddhi.debs2017;

import org.wso2.siddhi.debs2017.input.UnixConverter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Scanner;

/**
 * Created by sachini on 2/15/17.
 */
public class DataPublisherTest {
    public static void main(String[] args) {
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader("rdfData_extract_100m_time.csv"));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
              //  System.out.println(line);
                Scanner scanner = new Scanner(line);
                scanner.useDelimiter(",");
                while(scanner.hasNext())
                {

                    String machineName = scanner.next();
                    String timeStamp = scanner.next();
                    String time = scanner.next();
                    long timeValue = UnixConverter.getUnixTime(time);

                    String property = scanner.next();
                    double value = Double.parseDouble(scanner.next());
                    System.out.println(machineName + timeStamp + timeValue + property + value);

                }
            }
        } catch (Exception e){
            //log.info(e);
        }
    }
    }

