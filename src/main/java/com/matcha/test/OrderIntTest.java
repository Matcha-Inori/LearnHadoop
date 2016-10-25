package com.matcha.test;

import com.matcha.test.mapper.OrderIntTestMapper;
import com.matcha.test.reducer.OrderIntTestReducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

/**
 * Created by Administrator on 2016/10/24.
 */
public class OrderIntTest
{
    public static void main(String[] args)
    {
        try
        {
            File baseFile = new File(File.separator + "work" + File.separator + "hadoop");

            StringBuffer inputFileBuffer = new StringBuffer();
            inputFileBuffer.append("testData").append(File.separator).append("intList.txt");
            File inputFile = new File(baseFile, inputFileBuffer.toString());
            File outputFile = new File(baseFile, "testResult");

            Job job = Job.getInstance();
            job.setJarByClass(OrderIntTest.class);
            job.setJobName("Order Int Test");

            FileInputFormat.addInputPath(job, new Path(inputFile.toURI()));
            FileOutputFormat.setOutputPath(job, new Path(outputFile.toURI()));

            job.setMapperClass(OrderIntTestMapper.class);
            job.setReducerClass(OrderIntTestReducer.class);

            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(IntWritable.class);

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
        catch (IOException | InterruptedException | ClassNotFoundException e)
        {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
