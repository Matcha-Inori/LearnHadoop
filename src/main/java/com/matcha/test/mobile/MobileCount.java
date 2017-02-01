package com.matcha.test.mobile;

import com.matcha.test.mobile.mapper.MobileCountMapper;
import com.matcha.test.mobile.reducer.MobileCountReducer;
import com.matcha.test.mobile.writable.MobileCountWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by Matcha on 2016/12/29.
 */
public class MobileCount
{
    public static void main(String[] args)
    {
        if(args == null || args.length < 2)
            throw new IllegalArgumentException("must input all path");

        if(isEmpty(args[0]) || isEmpty(args[1]))
            throw new IllegalArgumentException("must input all path");

        try
        {
            String inputPath = args[0];
            String outputPath = args[1];

            Job job = Job.getInstance();

            job.setJarByClass(MobileCount.class);
            job.setJobName("Mobile Count Test");

            FileInputFormat.addInputPath(job, new Path(inputPath));
            FileOutputFormat.setOutputPath(job, new Path(outputPath));

            job.setMapperClass(MobileCountMapper.class);
            job.setReducerClass(MobileCountReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(MobileCountWritable.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(MobileCountWritable.class);

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
        catch (IOException | InterruptedException | ClassNotFoundException e)
        {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static boolean isEmpty(String str)
    {
        return str == null || str.length() == 0;
    }

    public enum MobileArgEnum
    {
        REPORT_TIME(0, "report time"),
        MOBILE_NUMBER(1, "mobile number"),
        AP_MAC(2, "ap mac"),
        AC_MAC(3, "ac mac"),
        HOST(4, "host"),
        SITE_TYPE(5, "site type"),
        UP_PACK_NUM(6, "up pack number"),
        DOWN_PACK_NUM(7, "down pack number"),
        UP_PAY_LOAD(8, "up pay load"),
        DOWN_PAY_LOAD(9, "down pay load"),
        HTTP_STATUS(10, "http status");

        private int index;
        private String description;

        private MobileArgEnum(int index, String description)
        {
            this.index = index;
            this.description = description;
        }

        public int getIndex()
        {
            return index;
        }

        public String getDescription()
        {
            return description;
        }
    }
}
