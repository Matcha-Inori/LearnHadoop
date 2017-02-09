package com.matcha.test.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Administrator on 2017/2/9.
 */
public class MulInputMapper extends Mapper<LongWritable, Text, Text, LongWritable>
{
    private Text text = new Text();
    private LongWritable longWritable = new LongWritable();

    @Override
    protected void map(LongWritable key,
                       Text value,
                       Context context) throws IOException, InterruptedException
    {
        String str = value.toString();
        String[] strArray = str.split(" ");
        text.set(strArray[0]);
        longWritable.set(Integer.parseInt(strArray[1]));
        context.write(text, longWritable);
    }
}
