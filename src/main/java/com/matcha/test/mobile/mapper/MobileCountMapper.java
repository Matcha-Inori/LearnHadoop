package com.matcha.test.mobile.mapper;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Matcha on 2016/12/29.
 */
public class MobileCountMapper extends Mapper<LongWritable, Text, Text, ArrayWritable>
{
    @Override
    protected void map(LongWritable key,
                       Text value,
                       Context context) throws IOException, InterruptedException
    {
        String argLine = value.toString();
        String[] args = argLine.split(" ");
        String cellNumber = args[];
    }
}
