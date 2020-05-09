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
    public void updateTimestamp(String maxTime) throws IOException {

        BufferedWriter writer = new BufferedWriter(new FileWriter(timeFilePath));
        writer.write(maxTime);

        System.out.println("Updated the timestamp with "+ maxTime +" in " + timeFilePath);
        writer.close();
    }

}
