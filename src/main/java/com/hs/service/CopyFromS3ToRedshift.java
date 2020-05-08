package com.hs.service;

import com.hs.constants.Constants;
import com.hs.util.PGUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class CopyFromS3ToRedshift {

    public void moveToRedshift() {
        Connection conn = null;
        Statement statement = null;
        try {
            Class.forName("com.amazon.redshift.jdbc42.Driver");

            String username = PGUtil.getProperty(Constants.REDSHIFT_USER);
            String password = PGUtil.getProperty(Constants.REDSHIFT_PASSWORD);
            String accessKey = PGUtil.getProperty(Constants.S3_KEY);
            String secretKey = PGUtil.getProperty(Constants.S3_SECRET);
            String bucketName = PGUtil.getProperty(Constants.S3_BUCKET);
            String tableName = PGUtil.getProperty(Constants.REDSHIFT_TABLE_NAME);
            String delimiter = PGUtil.getProperty(Constants.REDSHIFT_DELIMITER);
            String fileName = PGUtil.getProperty(Constants.REDSHIFT_FILENAME);


            System.out.println("Connecting to Redshift database......");

            conn = DriverManager.getConnection("jdbc:redshift://hiring-eval-cluster.ckvpogridq1r.us-east-1.redshift.amazonaws.com:5439/test_eval_navin", username, password);

            System.out.println("----------Connection Established to REDSHIFT----------");

            statement = conn.createStatement();

            String command = "COPY " + tableName + " from 's3://" + bucketName + "/" + fileName + "' CREDENTIALS 'aws_access_key_id=" + accessKey.trim() + ";aws_secret_access_key=" + secretKey + "' CSV DELIMITER '" + delimiter + "' ignoreheader 1 maxerror 10";

            System.out.println("command: " + command);
            System.out.println("==========Executing the copy command=========");

            statement.executeUpdate(command);

            System.out.println("Copied the file " + fileName + " from s3 to Redshift table");
            statement.close();
            conn.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {

                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
