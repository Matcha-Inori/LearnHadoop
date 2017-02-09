package com.matcha.learn.test;

import com.matcha.test.mapper.MulInputMapper;
import com.matcha.test.reducer.MulInputReducer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import java.io.File;
import java.net.URI;
import java.util.Arrays;

/**
 * Created by Administrator on 2017/2/9.
 */
public class MulInputMap extends Configured implements Tool
{
    @Override
    public int run(String[] args) throws Exception
    {
        URI baseURI = URI.create(args[0]);
        URI outputURI = baseURI.resolve(URI.create(args[1]));

        String[] inputs = Arrays.copyOfRange(args, 2, args.length);
        URI[] inputURIs = new URI[inputs.length];
        for(int index = 0;index < inputs.length;index++)
            inputURIs[index] = baseURI.resolve(URI.create(inputs[index]));

        Job job = Job.getInstance();

        job.setMapperClass(MulInputMapper.class);
        for(URI inputURI : inputURIs)
            MultipleInputs.addInputPath(job, new Path(inputURI), TextInputFormat.class);

        job.setReducerClass(MulInputReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileOutputFormat.setOutputPath(job, new Path(outputURI));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    @Test
    public void testMulInputMap() throws Exception
    {
        File basePath = new File("src/main/resources/");
        ToolRunner.run(
                new MulInputMap(),
                new String[]{
                        basePath.toURI().toString(),
                        "output/MulInputMapOutput",
                        "data/input1",
                        "data/input2",
                        "data/input3"
                }
        );
    }
}
