package com.hs.service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;

public class CopyFromS3ToRedshift {

    public void moveToRedshift(){
        Connection conn = null;
//        Statement statement = null;
        try {
            //Even postgresql driver will work too. You need to make sure to choose postgresql url instead of redshift.
            //Class.forName("org.postgresql.Driver");
            //Make sure to choose appropriate Redshift Jdbc driver and its jar in classpath
            Class.forName("com.amazon.redshift.jdbc.Driver");
            Properties props = new Properties();
            props.setProperty("user", "eval_user_navin");
            props.setProperty("password", "23fs$DWb*7W&d");

            System.out.println("\n\nconnecting to database...\n\n");
            //In case you are using postgreSQL jdbc driver.
            //conn = DriverManager.getConnection("jdbc:postgresql://********8-your-to-redshift.redshift.amazonaws.com:5439/example-database", props);

            conn = DriverManager.getConnection("jdbc:redshift://hiring-eval-cluster.ckvpogridq1r.us-east-1.redshift.amazonaws.com:5439/test_eval_navin", props);

            System.out.println("\n\nConnection made!\n\n");

//            statement = conn.createStatement();
//
//            String command = "COPY my_table from 's3://path/to/csv/example.csv' CREDENTIALS 'aws_access_key_id=******;aws_secret_access_key=********' CSV DELIMITER ',' ignoreheader 1";

            System.out.println("\n\nExecuting...\n\n");

//            statement.executeUpdate(command);
            //you must need to commit, if you realy want to have data saved, otherwise it will not appear if you query from other session.
//            conn.commit();
            System.out.println("\n\nThats all copy using simple JDBC.\n\n");
//            statement.close();
            conn.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
