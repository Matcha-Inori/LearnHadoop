package com.matcha.test.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Matcha on 2017/2/7.
 */
public class SequenceMapper extends Mapper<LongWritable, Text, LongWritable, Text>
{
    private LongWritable longWritable;
    private Text text;

    public SequenceMapper()
    {
        this.longWritable = new LongWritable();
        this.text = new Text();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        String valueStr = value.toString();
        String[] values = valueStr.split(" ");
        longWritable.set(Integer.parseInt(values[0]));
        text.set(values[3]);
        context.write(longWritable, text);
    }
}
