package com.hs.service;


import com.hs.constants.Constants;
import com.hs.util.PGUtil;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.FileWriter;
import java.sql.*;

public class CopyFromPostgres {

    /**
     * this method copies the changed contents in postgres to a file based ont he timestamp
     * @param fromTimestamp
     */
    public void copyFromPostgresToFile(String fromTimestamp, String toTimestamp) {

        String delimiter = PGUtil.getProperty(Constants.REDSHIFT_DELIMITER);

        Connection conn = null;
        try {


            System.out.println("In copyFromPostgresToFile() method");
            String hostName = PGUtil.getProperty(Constants.PG_HOST);
            String username = PGUtil.getProperty(Constants.PG_USERNAME);
            String password = PGUtil.getProperty(Constants.PG_PASSWORD);
            String port = PGUtil.getProperty(Constants.PG_PORT);
            String databaseName = PGUtil.getProperty(Constants.PG_DATABASE);
            String redshiftFileName = PGUtil.getProperty(Constants.REDSHIFT_FILENAME);

            //create a conenction
//            System.out.println(hostName+", "+username+", "+ password +", "+ port +", "+ databaseName);
            conn = DriverManager.getConnection("jdbc:postgresql://" + hostName + ":" + port + "/" + databaseName
                    , username, password);

            System.out.println("Connected to postgres");

            System.out.println("fromTimestamp is: " + fromTimestamp);
            System.out.println("toTimestamp is: " + toTimestamp);
            //COpymanager object to copy from pg to file
            CopyManager copyManager = new CopyManager((BaseConnection) conn);
            //the file in which the data is written
            FileWriter fileWriter = new FileWriter(redshiftFileName);
            String query = "COPY (select id,address_id,branch_id,created_at,updated_at,deliverydelay,rejectreason_id,fake,action_at,restaurant_fee,suspicious,case when note ~ E'[\\u0600-\\u06FF]' then null else replace(substring(note,1,220),'\n','') end as note,fakereason_id,local_id,pickup,user_id,discount,address_date,on_hold,state,failure_cause,user_payment_id,due_at,revenue,confirmed_at,fake_status,fake_reason,state_detail,amount,assigned_company_agent_id,device_id,operation_day,current_location_latitude,current_location_longitude,current_location_accuracy,case when CHAR_LENGTH(pos_order_reference) > 250 then substring(pos_order_reference,1,250) else pos_order_reference end as pos_order_reference,delivery_provider,dispatched_at,hungerstation_fee,source,coupon_id,prepared_at,integration_status,team_id,transmit_medium,vat_value,failed_at from public.orders where updated_at > '" + fromTimestamp.trim() + "' and updated_at <= '" + toTimestamp.trim() + "')  TO STDOUT WITH DELIMITER '"+delimiter+"' CSV HEADER";
            System.out.println("Copy command is: " + query);

            //where updated_at > '" + maxTimestamp.trim() + "'
            //copyout method
            copyManager.copyOut(query, fileWriter);
            //close the filewriter method
            fileWriter.close();

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Failed to create JDBC db connection " + e.toString() + e.getMessage());
        } finally {
            try {
                //connection close
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }

}