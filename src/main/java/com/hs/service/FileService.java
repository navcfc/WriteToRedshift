package com.hs.service;

import com.hs.constants.Constants;
import com.hs.util.PGUtil;

import java.io.*;
import java.sql.Timestamp;
import java.util.Calendar;

public class FileService {

    String timeFilePath = PGUtil.getProperty(Constants.TIME_FILE_PATH);

    /**
     * fetch the timestamp from the file
     *
     * @return the timestamp
     * @throws IOException
     */
    public String fetchTimestamp() throws IOException {
        File file = new File(timeFilePath);

        BufferedReader br = new BufferedReader(new FileReader(file));

        String st;
        st = br.readLine();

        br.close();
        return st;
    }

    /**
     * update the timestamp to current timestamp in the file
     * @throws IOException
     */
    public void updateTimestamp() throws IOException {
        File file = new File(timeFilePath);


        long retryDate = System.currentTimeMillis();

        int sec = -15;

        Timestamp original = new Timestamp(retryDate);
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(original.getTime());
        cal.add(Calendar.SECOND, sec);
        Timestamp before = new Timestamp(cal.getTime().getTime());

        System.out.println(original);
        System.out.println("Updated timestamp is: " +before);
        BufferedWriter writer = new BufferedWriter(new FileWriter(timeFilePath));
        writer.write(String.valueOf(before));

        writer.close();
    }

}
