package org.wso2.siddhi.debs2017.input;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.stream.input.InputHandler;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Scanner;

/**
 * Created by temp on 1/25/17.
 */
public class DataPublisher {

    private String fileName;
    private static final Logger log = Logger.getLogger(DataPublisher.class);
    private InputHandler inputHandler;

    /**
     * The constructor
     *
     * @param fileName the file name to read the data
     * @param inputHandler the inputhandler of the execution plan
     */
    public DataPublisher(String fileName, InputHandler inputHandler){

        this.fileName = fileName;
        this.inputHandler = inputHandler;

    }

    /**
     * Publishes data into Siddhi
     */
    public void publish() {

        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(fileName));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                System.out.println(line);
                Scanner scanner = new Scanner(line);
                scanner.useDelimiter(",");
                while(scanner.hasNext())
                {

                    String machineName = scanner.next();
                    String timeStamp = scanner.next();
                    String property = scanner.next();
                    double value = Double.parseDouble(scanner.next());
                    try {
                        inputHandler.send(new Object[]{machineName, timeStamp, property, value});

                    }catch (InterruptedException e){
                        e.printStackTrace();
                    }
                }
            }
        } catch (Exception e){
            log.info(e);
        }
    }





}
