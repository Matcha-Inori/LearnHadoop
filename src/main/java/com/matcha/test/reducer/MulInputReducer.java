package com.matcha.test.reducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Administrator on 2017/2/9.
 */
public class MulInputReducer extends Reducer<Text, LongWritable, Text, LongWritable>
{
    private LongWritable longWritable = new LongWritable();

    @Override
    protected void reduce(Text key,
                          Iterable<LongWritable> values,
                          Context context) throws IOException, InterruptedException
    {
        long total = 0L;
        for(LongWritable longWritable : values)
            total += longWritable.get();
        this.longWritable.set(total);
        context.write(key, this.longWritable);
    }
}
