package com.matcha.test.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by Administrator on 2016/10/24.
 */
public class OrderIntTestReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>
{
    @Override
    protected void reduce(IntWritable key,
                          Iterable<IntWritable> values,
                          Context context) throws IOException, InterruptedException
    {
        int max = Integer.MIN_VALUE;
        Iterator<IntWritable> valueIter = values.iterator();
        while(valueIter.hasNext())
        {
            max = Math.min(max, valueIter.next().get());
        }
        context.write(key, new IntWritable(max));
    }
}
