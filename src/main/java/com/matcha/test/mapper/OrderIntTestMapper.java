package com.matcha.test.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Administrator on 2016/10/24.
 */
public class OrderIntTestMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable>
{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        System.out.println("read data key - " + key.get() + " value - " + value.toString());
        String valueStr = value.toString();
        String[] information = valueStr.split(" ");
        int outputKey = Integer.parseInt(information[0]);
        int outputValue = Integer.parseInt(information[1]);
        System.out.println("out put data key - " + outputKey + " value - " + outputValue);
        context.write(new IntWritable(outputKey), new IntWritable(outputValue));
    }
}
