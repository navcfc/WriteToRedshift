package com.hs.service;



import com.hs.constants.Constants;
import com.hs.util.PGUtil;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.FileWriter;
import java.sql.*;

public class CopyFromPostgres {

    public String copyFromPostgresToFile() {

        StringBuilder builder = new StringBuilder();
        Connection conn = null;
        // auto close connection
        try {


            System.out.println("In copyFromPostgresToFile() method");
            String hostName = PGUtil.getProperty(Constants.PG_HOST);
            String username = PGUtil.getProperty(Constants.PG_USERNAME);
            String password = PGUtil.getProperty(Constants.PG_PASSWORD);
            String port = PGUtil.getProperty(Constants.PG_PORT);
            String databaseName = PGUtil.getProperty(Constants.PG_DATABASE);

//            System.out.println(hostName+", "+username+", "+ password +", "+ port +", "+ databaseName);
            conn = DriverManager.getConnection("jdbc:postgresql://"+hostName + ":"+port+"/"+databaseName
                    ,username , password);

            System.out.println("Connected to postgres");

            CopyManager copyManager = new CopyManager((BaseConnection) conn);
            FileWriter fileWriter = new FileWriter("orders.csv");
            copyManager.copyOut("COPY (select * from public.orders limit 100) TO STDOUT WITH DELIMITER '|' CSV HEADER", fileWriter);



        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Failed to create JDBC db connection " + e.toString() + e.getMessage());
        }
        finally {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return builder.toString();
    }

}