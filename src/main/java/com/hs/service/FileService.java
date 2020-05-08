package com.hs.service;

import com.hs.constants.Constants;
import com.hs.util.PGUtil;

import java.io.*;
import java.sql.Timestamp;

public class FileService {

    String timeFilePath = PGUtil.getProperty(Constants.TIME_FILE_PATH);

    public String fetchTimestamp() throws IOException {
        File file = new File(timeFilePath);

        BufferedReader br = new BufferedReader(new FileReader(file));

        String st;
        st = br.readLine();

        br.close();
        return st;
    }

    public void updateTimestamp() throws IOException {
        File file = new File(timeFilePath);


        BufferedWriter writer = new BufferedWriter(new FileWriter(timeFilePath));
        writer.write(String.valueOf(new Timestamp(System.currentTimeMillis())));

        writer.close();
    }

}
