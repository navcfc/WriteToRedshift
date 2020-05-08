//package com.hs.service;
//
//
//import com.hs.constants.Constants;
//import com.hs.util.PGUtil;
//import org.postgresql.copy.CopyManager;
//import org.postgresql.core.BaseConnection;
//
//import java.io.BufferedWriter;
//import java.io.FileWriter;
//import java.sql.*;
//
//public class CopyFromPostgresNotWorking {
//
//    public void copyToFile() {
////    public static void main(String args[]) {
//
//        String maxTimestamp="";
////        String timeFilePath = PGUtil.getProperty(Constants.TIME_FILE_PATH);
//        String delimiter = "|";
//
//        Connection conn = null;
//        // auto close connection
//        try {
//
//
//            System.out.println("In copyFromPostgresToFile() method");
//            String hostName = PGUtil.getProperty(Constants.PG_HOST);
//            String username = PGUtil.getProperty(Constants.PG_USERNAME);
//            String password = PGUtil.getProperty(Constants.PG_PASSWORD);
//            String port = PGUtil.getProperty(Constants.PG_PORT);
//            String databaseName = PGUtil.getProperty(Constants.PG_DATABASE);
//
////            System.out.println(hostName+", "+username+", "+ password +", "+ port +", "+ databaseName);
////            conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres",  "postgres", "admin");
//            conn = DriverManager.getConnection("jdbc:postgresql://" + hostName + ":" + port + "/" + databaseName
//                    , username, password);
//            System.out.println("Connected to postgres");
//
//            System.out.println("maxTimestamp is: " + maxTimestamp);
//
////            CopyManager copyManager = new CopyManager((BaseConnection) conn);
//            FileWriter fileWriter = new FileWriter("testwrite.csv");
//
////            String query = "COPY (select id,address_id,branch_id,created_at,updated_at,deliverydelay,rejectreason_id,fake,action_at,restaurant_fee,suspicious,case when note ~ E'[\\u0600-\\u06FF]' then null else note end as note,fakereason_id,local_id,pickup,user_id,discount,address_date,on_hold,state,failure_cause,user_payment_id,due_at,revenue,confirmed_at,fake_status,fake_reason,state_detail,amount,assigned_company_agent_id,device_id,operation_day,current_location_latitude,current_location_longitude,current_location_accuracy,case when CHAR_LENGTH(pos_order_reference) > 250 then substring(pos_order_reference,1,250) else pos_order_reference end as pos_order_reference,delivery_provider,dispatched_at,hungerstation_fee,source,coupon_id,prepared_at,integration_status,team_id,transmit_medium,vat_value,failed_at from public.orders where updated_at >'" + maxTimestamp + "')  TO STDOUT WITH (FORMAT CSV) ";
//            BufferedWriter out = new BufferedWriter(fileWriter);
//            String query = "select * from public.orders limit 30000";
//            Statement st = conn.createStatement();
//            ResultSet rs = st.executeQuery(query);
//
////            ////
////
////            conn.setAutoCommit(false);
////            Statement st = conn.createStatement();
////
////// Turn use of the cursor on.
////            st.setFetchSize(50);
////            ResultSet rs = st.executeQuery(query);
////            while (rs.next()) {
//////                System.out.println("a row was returned. : " + rs.getInt(1));
////            }
////            rs.close();
////
////// Turn the cursor off.
//////            st.setFetchSize(0);
//////            rs = st.executeQuery(query);
//////            while (rs.next()) {
//////                System.out.print("many rows were returned. : " + rs.getInt(1));
//////            }
//////            rs.close();
////
////// Close the statement.
////            st.close();
////
////            ///
//            while (rs.next())
//            {
////                System.out.print("Column 1 returned ");
//                String toWrite = rs.getInt("id") + delimiter + rs.getInt("address_id") + delimiter + rs.getInt("branch_id") + delimiter + rs.getTimestamp("created_at") + delimiter + rs.getTimestamp("updated_at") + delimiter + rs.getInt("deliverydelay") + delimiter + rs.getInt("rejectreason_id") + delimiter + rs.getString("fake") + delimiter + rs.getTimestamp("action_at") + delimiter + rs.getDouble("restaurant_fee") + delimiter + rs.getString("suspicious") + delimiter + rs.getString("note") + delimiter + rs.getInt("fakereason_id") + delimiter + rs.getInt("local_id") + delimiter + rs.getString("pickup") + delimiter + rs.getInt("user_id") + delimiter + rs.getDouble("discount") + delimiter + rs.getTimestamp("address_date") + delimiter + rs.getString("on_hold") + delimiter + rs.getInt("state") + delimiter + rs.getInt("failure_cause") + delimiter + rs.getInt("user_payment_id") + delimiter + rs.getTimestamp("due_at") + delimiter + rs.getDouble("revenue") + delimiter + rs.getTimestamp("confirmed_at") + delimiter + rs.getInt("fake_status") + delimiter + rs.getInt("fake_reason") + delimiter + rs.getInt("state_detail") + delimiter + rs.getDouble("amount") + delimiter + rs.getInt("assigned_company_agent_id") + delimiter + rs.getInt("device_id") + delimiter + rs.getDate("operation_day") + delimiter + rs.getDouble("current_location_latitude") + delimiter + rs.getDouble("current_location_longitude") + delimiter + rs.getInt("current_location_accuracy") + delimiter + rs.getString("pos_order_reference") + delimiter + rs.getInt("delivery_provider") + delimiter + rs.getTimestamp("dispatched_at") + delimiter + rs.getDouble("hungerstation_fee") + delimiter + rs.getInt("source") + delimiter + rs.getInt("coupon_id") + delimiter + rs.getTimestamp("prepared_at") + delimiter + rs.getInt("integration_status") + delimiter + rs.getInt("team_id") + delimiter + rs.getInt("transmit_medium") + delimiter + rs.getDouble("vat_value") + delimiter + rs.getTimestamp("failed_at") + "\n";
//                System.out.println(toWrite);
//                out.write(toWrite);
////                System.out.println(toWrite);
////            out.newLine();
//            }
//            out.close();
//            rs.close();
//            st.close();
//            System.out.println("Completed writing to file");
////            copyManager.copyOut(query, fileWriter);
//
//
//        } catch (Exception e) {
//            e.printStackTrace();
//            System.out.println("Failed to create JDBC db connection " + e.toString() + e.getMessage());
//        } finally {
//            try {
////                conn.setAutoCommit(true);
//
//                conn.close();
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//        }
//
//    }
//
//}