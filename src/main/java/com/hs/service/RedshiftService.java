package com.hs.service;

import com.hs.constants.Constants;
import com.hs.util.PGUtil;

import java.sql.*;

public class RedshiftService {


//    public static void main(String args[]) throws SQLException, ClassNotFoundException {
//
//        PGUtil.loadConfigFile("C:\\Users\\DELL\\hungerstation\\application.properties");
//        RedshiftService service = new RedshiftService();
//
//        service.truncateRedshiftStageTable();
//        service.moveToRedshiftStagingTable();
//        service.insertNewRecordsIntoRedshiftFoundation();
//        service.updateFoundationTable();
//    }
    public Connection getConnectionToRedshift() throws ClassNotFoundException, SQLException {
        Connection conn = null;

        {
            Class.forName("com.amazon.redshift.jdbc42.Driver");

            String username = PGUtil.getProperty(Constants.REDSHIFT_USER);
            String password = PGUtil.getProperty(Constants.REDSHIFT_PASSWORD);



            System.out.println("Connecting to Redshift database......");

            conn = DriverManager.getConnection("jdbc:redshift://hiring-eval-cluster.ckvpogridq1r.us-east-1.redshift.amazonaws.com:5439/test_eval_navin", username, password);

            System.out.println("----------Connection Established to REDSHIFT----------");


        }

        return conn;
    }
    public void moveToRedshiftStagingTable() throws SQLException, ClassNotFoundException {

        Statement statement = null;

        String accessKey = PGUtil.getProperty(Constants.S3_KEY);
        String secretKey = PGUtil.getProperty(Constants.S3_SECRET);
        String bucketName = PGUtil.getProperty(Constants.S3_BUCKET);
        String stageTableName = PGUtil.getProperty(Constants.REDSHIFT_STAGE_TABLE_NAME);
        String delimiter = PGUtil.getProperty(Constants.REDSHIFT_DELIMITER);
        String fileName = PGUtil.getProperty(Constants.REDSHIFT_FILENAME);
        Connection conn = getConnectionToRedshift();

        try {

            statement = conn.createStatement();

            String command = "COPY " + stageTableName + " from 's3://" + bucketName + "/" + fileName + "' CREDENTIALS 'aws_access_key_id=" + accessKey.trim() + ";aws_secret_access_key=" + secretKey + "' CSV DELIMITER '" + delimiter + "' ignoreheader 1 maxerror 10";

            System.out.println("command: " + command);
            System.out.println("==========Executing the copy command=========");

            statement.executeUpdate(command);

            System.out.println("Copied the file " + fileName + " from s3 to "+stageTableName+" Redshift table");
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


    public void truncateRedshiftStageTable() throws SQLException, ClassNotFoundException {

        Statement statement = null;

        String stageTableName = PGUtil.getProperty(Constants.REDSHIFT_STAGE_TABLE_NAME);
        Connection conn = getConnectionToRedshift();

        try {

            statement = conn.createStatement();

            String command = "truncate table " + stageTableName;

            System.out.println("command: " + command);
            System.out.println("==========Executing the truncate table command=========");

            int countTruncate = statement.executeUpdate(command);

            System.out.println(countTruncate + " records truncated from the " + stageTableName);
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



    public void insertNewRecordsIntoRedshiftFoundation() throws SQLException, ClassNotFoundException {

        Statement statement = null;

        String tableName = PGUtil.getProperty(Constants.REDSHIFT_TABLE_NAME);
        String stageTableName = PGUtil.getProperty(Constants.REDSHIFT_STAGE_TABLE_NAME);
        Connection conn = getConnectionToRedshift();

        try {

            statement = conn.createStatement();

            String command = "insert into "+tableName+" (select stage.* from "+stageTableName+" stage left join "+tableName+" foundation on stage.id = foundation.id where foundation.id is  null) ";

            System.out.println("insert command: " + command);
            System.out.println("==========Executing the insert table command=========");

            int countInserted = statement.executeUpdate(command);

            System.out.println("Inserted "+countInserted+" into " + tableName +" from " + stageTableName);
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



    public void updateFoundationTable() throws SQLException, ClassNotFoundException {

        Statement selectStatement = null;
        Statement deleteStatement = null;
        Statement insertStatement = null;
        String tableName = PGUtil.getProperty(Constants.REDSHIFT_TABLE_NAME);
        String stageTableName = PGUtil.getProperty(Constants.REDSHIFT_STAGE_TABLE_NAME);
        Connection conn = getConnectionToRedshift();

        try {

            //Selecting all the ids which have to be udpated
            selectStatement = conn.createStatement();

            String command = "select stage.id from "+stageTableName+" stage left join "+tableName+" foundation on stage.id = foundation.id where foundation.id is not null and stage.updated_at > foundation.updated_at ";

            System.out.println("select command to see all update records: " + command);
            System.out.println("==========Executing the select table command=========");

            ResultSet rs = selectStatement.executeQuery(command);
            StringBuilder builder = new StringBuilder();
            int i = 0;
            while (rs.next()) {
                i++;
               builder.append("'"+rs.getInt(1)+"',");
            }

            if(i>1)
                builder.deleteCharAt(builder.length()-1);

            System.out.println("List to delete from foundation table for update is: " + builder.toString());
            selectStatement.close();
            rs.close();


            //Time to delete the selected ids from the foundation table

            if(i > 1) {
                deleteStatement = conn.createStatement();

                String deletecommand = "delete from " + tableName + " where id in (" + builder.toString() + ")";

                System.out.println("delete command to delete all updated records: " + deletecommand);
                System.out.println("==========Executing the delete table command=========");

                int countDelete = deleteStatement.executeUpdate(deletecommand);
                System.out.println(countDelete + " records deleted from the foundation table " + builder.toString());
                deleteStatement.close();


                //Time to insert the new updated records
                insertStatement = conn.createStatement();

                String insertCommand = "insert into dwh.orders ( select stage.* from dwh.orders_stage stage where id in(" + builder.toString() + ") )";

                System.out.println("insert command to insert all updated records: " + insertCommand);
                System.out.println("==========Executing the delete table command=========");

                int countInsert = insertStatement.executeUpdate(insertCommand);
                System.out.println(countInsert + " records updated in the foundation table ");
                insertStatement.close();

            }
            conn.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {

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
