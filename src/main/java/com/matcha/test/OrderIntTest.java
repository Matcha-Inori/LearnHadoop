package com.matcha.test;

import com.matcha.test.mapper.OrderIntTestMapper;
import com.matcha.test.reducer.OrderIntTestReducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * Created by Administrator on 2016/10/24.
 */
public class OrderIntTest
{
    public static void main(String[] args)
    {
        try
        {
            URL baseURL = OrderIntTest.class.getClassLoader().getResource("");
            URI baseURI = baseURL.toURI().resolve("../../src/main/resources/");
            URI inputFileURI = baseURI.resolve("data/intList.txt");
            URI outputFileURI = baseURI.resolve("result");

            Job job = Job.getInstance();
            job.setJarByClass(OrderIntTest.class);
            job.setJobName("Order Int Test");

            FileInputFormat.addInputPath(job, new Path(inputFileURI));
            FileOutputFormat.setOutputPath(job, new Path(outputFileURI));

            job.setMapperClass(OrderIntTestMapper.class);
            job.setReducerClass(OrderIntTestReducer.class);

            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(IntWritable.class);

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
        catch (IOException | URISyntaxException | InterruptedException | ClassNotFoundException e)
        {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
