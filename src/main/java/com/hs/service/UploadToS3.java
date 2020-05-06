package com.hs.service;


import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.hs.constants.Constants;
import com.hs.util.PGUtil;

import java.io.File;

public class UploadToS3 {

    public void uploadFile() {

        String accessKey = PGUtil.getProperty(Constants.S3_KEY);
        String secretKey = PGUtil.getProperty(Constants.S3_SECRET);
        String bucketName = PGUtil.getProperty(Constants.S3_BUCKET);
        String filePath = PGUtil.getProperty(Constants.S3_INPUT_PATH);

        AWSCredentials awsCredentials = new BasicAWSCredentials(accessKey,secretKey);
        AmazonS3 s3 = new AmazonS3Client(awsCredentials);
//        AmazonS3 s3 = new AmazonS3Client();
        Region usEast1 = Region.getRegion(Regions.US_EAST_1);
        s3.setRegion(usEast1);


//        String bucketName = "hiring-eval-navin";
        String key = "orders.csv";

        System.out.println("===========================================");
        System.out.println("Getting Started with Amazon S3");
        System.out.println("===========================================\n");

        System.out.println("Uploading a new object to S3 from a file\n");
        s3.putObject(new PutObjectRequest(bucketName, key, new File(filePath)));

        System.out.println("Uploaded");

        s3.shutdown();
    }

}
