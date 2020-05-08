package com.hs.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.*;

public class ConsumeFromKafka {

    public Properties getProperties() throws IOException {
        Properties properties = new Properties();

        InputStream inputStream;
        inputStream = ConsumeFromKafka.class.getClassLoader().getResourceAsStream("kafka.properties");

        System.out.println(inputStream);
        if (inputStream != null) {
            properties.load(inputStream);
        } else {
            throw new FileNotFoundException("property file not found in the classpath");
        }

        return properties;

    }


    public void readKafkaTopic(String topicName) throws IOException, SQLException, ClassNotFoundException {
        //Kafka consumer configuration settings

        Properties properties = getProperties();

        KafkaConsumer<String, String> consumer = new KafkaConsumer
                <String, String>(properties);

        //Kafka Consumer subscribes list of topics here.
        consumer.subscribe(Arrays.asList(topicName));

        //print the topic name
        System.out.println("Subscribed to topic " + topicName);
        int i = 0;

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                // print the offset,key and value for the consumer records.
                System.out.printf("offset = %d, key = %s, value = %s\n",
                        record.offset(), record.key(), record.value());



                if (record.key().trim().equalsIgnoreCase("count") && Integer.valueOf(record.value().trim())>0){
                    System.out.println("In the count check if clause");

                    FileService fileService = new FileService();
                    String maxTimestamp = fileService.fetchTimestamp();
//                    fileService.updateTimestamp();
                    CopyFromPostgres copyFromPostgres = new CopyFromPostgres();
                    copyFromPostgres.copyFromPostgresToFile(maxTimestamp);
                    UploadToS3 uploadToS3 = new UploadToS3();
                    uploadToS3.uploadFile();
                    RedshiftService service = new RedshiftService();

                    service.truncateRedshiftStageTable();
                    service.moveToRedshiftStagingTable();
                    service.insertNewRecordsIntoRedshiftFoundation();
                    service.updateFoundationTable();

                }
            }}
    }

}
