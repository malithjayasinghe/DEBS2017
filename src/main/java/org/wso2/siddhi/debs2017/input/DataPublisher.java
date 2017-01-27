package org.wso2.siddhi.debs2017.input;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.stream.input.InputHandler;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Scanner;

/**
 * Created by temp on 1/25/17.
 */
public class DataPublisher {

    private String fileName;
    private static final Logger log = Logger.getLogger(DataPublisher.class);
    private InputHandler inputHandler;

    public DataPublisher(String fileName, InputHandler inputHandler){

        this.fileName = fileName;
        this.inputHandler = inputHandler;

    }



    public void publish() {

        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(fileName));
            String line = null;
            Scanner scanner = null;
            while ((line = bufferedReader.readLine()) != null) {
                scanner = new Scanner(line);
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
