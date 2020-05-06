package com.hs;// Java program to demonstrate
//schedule method calls of Timer class 

import com.hs.service.ConsumeFromKafka;
import com.hs.service.CopyFromPostgres;
import com.hs.service.CopyFromS3ToRedshift;
import com.hs.service.UploadToS3;
import com.hs.util.PGUtil;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

//class Helper extends TimerTask
//{
//    public static int i = 0;
//    public void run()
//    {
////        CopyFromPostgres postgreSqlExample = new CopyFromPostgres();
////        String kafkaMessage = postgreSqlExample.runQuery();
////        System.out.println("\nkafkaMessage: " + kafkaMessage);
//        System.out.println("Timer ran " + ++i);
//    }
//}

public class Test
{
    public static void main(String[] args) throws IOException {

        //working
        if(args.length != 1){
            System.out.println("Invalid number of imput params: Please follow the " +
                    "ordering: 1.Properties file path");
            System.exit(-1);
        }
        String filePath = args[0];
        PGUtil.loadConfigFile(filePath);
//
//        CopyFromPostgres postgreSqlExample = new CopyFromPostgres();
//        String kafkaMessage = postgreSqlExample.copyFromPostgresToFile();
//        System.out.println("\nkafkaMessage: " + kafkaMessage);




        //working
//        UploadToS3 uploadToS3 = new UploadToS3();
//        uploadToS3.uploadFile();


        CopyFromS3ToRedshift copyFromS3ToRedshift = new CopyFromS3ToRedshift();
        copyFromS3ToRedshift.moveToRedshift();

        //working
//        ConsumeFromKafka consumeFromKafka = new ConsumeFromKafka();
//
//        consumeFromKafka.readKafkaTopic("test");


//       Timer timer = new Timer();
//        TimerTask task = new Helper();
//
//        timer.schedule(task, 2000, 5000);

    }
} 