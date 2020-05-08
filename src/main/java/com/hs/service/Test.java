package com.hs.service;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Test {
    private static Random rand = new Random();
    public static void main(String args[]){
        //id int, name varchar(30), gender varchar(30), updated_at timestamp, created_at timestamp, test_flag boolean, location varchar(30), age bigint

        List<String> gender = new ArrayList<String>();
        gender.add("male");
        gender.add("female");

        List<String> timestamps = new ArrayList<>();
        timestamps.add("2021-05-03 19:40:40");
        timestamps.add("2021-05-02 19:40:40");
        timestamps.add("2021-05-03 19:41:40");
        timestamps.add("2021-05-03 19:23:40");
        timestamps.add("2021-05-01 18:40:40");
        timestamps.add("2021-05-04 12:40:40");
        timestamps.add("2021-05-01 19:20:40");

        List<String> test = new ArrayList<String>();
        test.add("true");
        test.add("false");


        List<String> location = new ArrayList<String>();
        location.add("india");
        location.add("sri lanka");
        location.add("ksa");
        location.add("dubai");
        location.add("usa");
        location.add("england");
        location.add("pakistan");
        location.add("australia");


        List<String> names = new ArrayList<String>();
        names.add("ravi");
        names.add("kisan");
        names.add("arnab");
        names.add("goo");
        names.add("tendul");
        names.add("vivek");
        names.add("ram");
        names.add("gajo");


//        int i = 1;
        Test obj = new Test();
        for( int i = 1; i < 100; i ++){
//            System.out.println(obj.getRandomList(gender));
            System.out.println("insert into stage values("+rand.nextInt(12000)+",'"+obj.getRandomList(names)+
                    "','"+obj.getRandomList(gender)+"','"+obj.getRandomList(timestamps)+
                    "','"+obj.getRandomList(timestamps)+"',"+ obj.getRandomList(test)+
                    ",'"+ obj.getRandomList(location)+"',"+ rand.nextInt(40)+");");

        }
    }

    public String getRandomList(List<String> list) {

        //0-4
        int index = rand.nextInt(list.size());
//        System.out.println("\nIndex :" + index );
        return list.get(index);

    }
}
