package com.matcha.test.mobile.reducer;

import com.matcha.test.mobile.writable.MobileCountWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Matcha on 2016/12/30.
 */
public class MobileCountReducer extends Reducer<Text, MobileCountWritable, Text, MobileCountWritable>
{
    private MobileCountWritable totalCountWritable;

    public MobileCountReducer()
    {
        this.totalCountWritable = new MobileCountWritable();
    }

    @Override
    protected void reduce(Text key,
                          Iterable<MobileCountWritable> values,
                          Context context) throws IOException, InterruptedException
    {
        this.totalCountWritable.set(key, 0, 0);
        for(MobileCountWritable mobileCountWritable : values)
            totalCountWritable.add(mobileCountWritable);
        context.write(key, totalCountWritable);
    }
}
