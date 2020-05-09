package com.hs.service;


import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.hs.constants.Constants;
import com.hs.util.PGUtil;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

public class UploadToS3 {

    /**
     * This method uploads the file from postgres to S3
     */
    public void uploadFile() {

        String accessKey = PGUtil.getProperty(Constants.S3_KEY);
        String secretKey = PGUtil.getProperty(Constants.S3_SECRET);
        String bucketName = PGUtil.getProperty(Constants.S3_BUCKET);
        String filePath = PGUtil.getProperty(Constants.S3_INPUT_PATH);

        //Create s3 objects
        AWSCredentials awsCredentials = new BasicAWSCredentials(accessKey,secretKey);
        AmazonS3 s3 = new AmazonS3Client(awsCredentials);
//        AmazonS3 s3 = new AmazonS3Client();
        Region usEast1 = Region.getRegion(Regions.US_EAST_1);
        s3.setRegion(usEast1);


    //Specify the file name to which it has to be copied in s3
        String key = "orders.csv";

        System.out.println("===========================================");
        System.out.println("Uploading a new object to S3 from a file");
        System.out.println("===========================================\n");

        s3.putObject(new PutObjectRequest(bucketName, key, new File(filePath)));

        System.out.println(key + " file uploaded in s3");

        //Adding timestamp to the file that wil be archived in s3
        long timeInMillis = System.currentTimeMillis();
        SimpleDateFormat sdf = new SimpleDateFormat("dd_MM_yy_HH_mm_ss");
        Date resultdate = new Date(timeInMillis);
        System.out.println(sdf.format("Result date of file name is: " + resultdate));
        //Copying the content to a new file so as to archive it.
        CopyObjectRequest copyObjRequest = new CopyObjectRequest(bucketName,
                key, bucketName, key+"_"+sdf.format(resultdate).toString());
        s3.copyObject(copyObjRequest);

        //shutdown the s3 connection
        s3.shutdown();
    }

}
