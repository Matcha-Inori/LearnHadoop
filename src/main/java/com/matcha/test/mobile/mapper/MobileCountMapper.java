package com.matcha.test.mobile.mapper;

import com.matcha.test.mobile.MobileCount;
import com.matcha.test.mobile.writable.MobileCountWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Matcha on 2016/12/29.
 */
public class MobileCountMapper extends Mapper<LongWritable, Text, Text, MobileCountWritable>
{
    private Text mobileNumber;
    private MobileCountWritable mobileCountWritable;

    public MobileCountMapper()
    {
        this.mobileNumber = new Text();
        this.mobileCountWritable = new MobileCountWritable();
    }

    @Override
    protected void map(LongWritable key,
                       Text value,
                       Context context) throws IOException, InterruptedException
    {
        String argLine = value.toString();
        String[] args = argLine.split("\\t");
        mobileNumber.set(args[MobileCount.MobileArgEnum.MOBILE_NUMBER.getIndex()]);
        mobileCountWritable.set(
                mobileNumber,
                Long.parseLong(args[MobileCount.MobileArgEnum.UP_PAY_LOAD.getIndex()]),
                Long.parseLong(args[MobileCount.MobileArgEnum.DOWN_PAY_LOAD.getIndex()])
        );
        context.write(mobileNumber, mobileCountWritable);
    }
}
