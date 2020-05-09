package com.hs;// Java program to demonstrate
//schedule method calls of Timer class 

import com.hs.service.*;
import com.hs.util.PGUtil;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;


public class RedshiftPipeline {
    public static void main(String[] args) throws IOException {

        //check the argument length
        if (args.length != 1) {
            System.out.println("Invalid number of imput params: Please follow the " +
                    "ordering: 1.Properties file path");
            System.exit(-1);
        }
        String filePath = args[0];
        PGUtil.loadConfigFile(filePath);

        //consume the count message frmo kafka
        ConsumeFromKafka consumeFromKafka = new ConsumeFromKafka();

        try {
            //read the kafka topic
            consumeFromKafka.readKafkaTopic("test");
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }


    }
} 