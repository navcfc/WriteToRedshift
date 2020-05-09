package com.hs.service;

import com.hs.constants.Constants;
import com.hs.util.PGUtil;
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


    /**
     *  consumer method to read the kafka topic
     */
    public void readKafkaTopic() throws IOException, SQLException, ClassNotFoundException {
        //Kafka consumer configuration settings

        String topicName = PGUtil.getProperty(Constants.TOPIC_NAME);

        Properties properties = getProperties();

        KafkaConsumer<String, String> consumer = new KafkaConsumer
                <String, String>(properties);

        //Kafka Consumer subscribes to list of topics here.
        consumer.subscribe(Arrays.asList(topicName));

        //print the topic name
        System.out.println("Subscribed to topic " + topicName);
        int i = 0;

        while (true) {
            //polling with a timeout of 100 ms
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                // print the offset,key and value for the consumer records.
                System.out.printf("offset = %d, key = %s, value = %s\n",
                        record.offset(), record.key(), record.value());

                // check if count has any values to trigger the process
                if (record.key().trim().equalsIgnoreCase("count") && Integer.valueOf(record.value().split(",")[0].trim())>0){
                    System.out.println("In the count check if clause");

                    FileService fileService = new FileService();
                    //fetch the timestamp from the file
                    String maxTimestamp = fileService.fetchTimestamp();
                    //update the timestamp with the current timestamp
                    fileService.updateTimestamp(record.value().split(",")[1].trim());
                    CopyFromPostgres copyFromPostgres = new CopyFromPostgres();
                    //copy all the changes that have happened in the source table
                    copyFromPostgres.copyFromPostgresToFile(maxTimestamp);
                    UploadToS3 uploadToS3 = new UploadToS3();
                    //upload the copied file to s3
                    uploadToS3.uploadFile();
                    RedshiftService service = new RedshiftService();
                    //truncate the staging redshift table to copy from s3
                    service.truncateRedshiftStageTable();
                    //copy from s3 to staging table
                    service.moveToRedshiftStagingTable();
                    //insert new records direcly into foundation reshift table
                    service.insertNewRecordsIntoRedshiftFoundation();
                    //update the foundation table
                    service.updateFoundationTable();

                }
            }}
    }

}
