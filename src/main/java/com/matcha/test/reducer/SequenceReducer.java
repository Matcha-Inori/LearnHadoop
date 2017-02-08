package com.matcha.test.reducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by Matcha on 2017/2/8.
 */
public class SequenceReducer extends Reducer<LongWritable, Text, LongWritable, Text>
{
    private Text text = new Text();

    @Override
    protected void reduce(LongWritable key,
                          Iterable<Text> values,
                          Context context) throws IOException, InterruptedException
    {
        StringBuffer stringBuffer = new StringBuffer();
        Iterator<Text> iterator = values.iterator();
        Text text;
        while(iterator.hasNext())
        {
            text = iterator.next();
            stringBuffer.append(text.toString());
            if(iterator.hasNext())
                stringBuffer.append(", ");
        }
        this.text.set(stringBuffer.toString());
        context.write(key, this.text);
    }
}
