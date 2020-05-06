package com.hs.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class PGUtil {

    private static String configFilePath = "";
    public static Properties pgProps = null;

    public static String getProperty(String name){
        String str = "";
        str = pgProps.get(name).toString();
        return str;

    }

    public static void loadConfigFile(String filePath){
        configFilePath = filePath;
        pgProps = new Properties();

        try {
            pgProps.load(new FileInputStream(configFilePath));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public String getPropertyForDB(String prop) {
        Properties properties = new Properties();

        try {
            properties.load(PGUtil.class.getClassLoader().getResourceAsStream("kafka.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        String propertyValue = properties.getProperty(prop).toString();

        return propertyValue;

    }
}
