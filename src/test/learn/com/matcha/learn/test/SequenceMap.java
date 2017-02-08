package com.matcha.learn.test;

import com.matcha.test.mapper.SequenceMapper;
import com.matcha.test.reducer.SequenceReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import java.io.File;
import java.net.URI;

/**
 * Created by Matcha on 2017/2/7.
 */
public class SequenceMap extends Configured implements Tool
{
    @Override
    public int run(String[] args) throws Exception
    {
        //这样可以在本地运行MapReduce任务，但是注意，如果没有reducer的话，那么map的输出并不会排序，因为使用的输出RecordWriter
        //并不一样
        URI baseURI = URI.create(args[0]);
        URI intPutURI = baseURI.resolve(URI.create(args[1]));
        URI outPutURI = baseURI.resolve(URI.create(args[2]));

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "TestSequenceMap");

        job.setMapperClass(SequenceMapper.class);

        FileInputFormat.addInputPath(job, new Path(intPutURI));

        job.setReducerClass(SequenceReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setCompressOutput(job, true);
        //GZip在这里需要hadoop的原生库，唔，直接换一个
        SequenceFileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
        SequenceFileOutputFormat.setOutputPath(job, new Path(outPutURI));

//        FileOutputFormat.setOutputPath(job, new Path(outPutURI));

//        job.setNumReduceTasks(0);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    @Test
    public void test() throws Exception
    {
        File basePath = new File("src/main/resources/");
        ToolRunner.run(
                new SequenceMap(),
                new String[]{
                        basePath.toURI().toString(),
                        "data/testSequenceMapper.txt",
                        "output/SequenceMapOutput"
                }
        );

        URI sequenceFileURI = basePath.toURI().resolve(URI.create("output/SequenceMapOutput/part-r-00000"));
        Path sequenceFilePath = new Path(sequenceFileURI);
        Configuration configuration = new Configuration();
        SequenceFile.Reader reader = new SequenceFile.Reader(
                configuration,
                SequenceFile.Reader.file(sequenceFilePath)
        );
        LongWritable longWritable = new LongWritable();
        Text text = new Text();
        while(reader.next(longWritable, text))
            System.out.println(String.format("%d\t-\t%s", longWritable.get(), text.toString()));
    }
}
