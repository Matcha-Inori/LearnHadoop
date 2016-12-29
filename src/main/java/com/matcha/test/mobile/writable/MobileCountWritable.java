package com.matcha.test.mobile.writable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Matcha on 2016/12/29.
 */
public class MobileCountWritable implements WritableComparable<MobileCountWritable>
{
    private Text mobileNumber;
    private LongWritable upPayLoad;
    private LongWritable downPayLoad;
    private LongWritable totalPayLoad;

    public MobileCountWritable(MobileCountWritable otherMobileCount)
    {
        this(
                otherMobileCount.getMobileNumber(),
                otherMobileCount.getUpPayLoad(),
                otherMobileCount.getDownPayLoad()
        );
    }

    public MobileCountWritable(Text mobileNumber,
                               LongWritable upPayLoad,
                               LongWritable downPayLoad)
    {
        this(mobileNumber.toString(), upPayLoad.get(), downPayLoad.get());
    }

    public MobileCountWritable(String mobileNumber,
                               long upPayLoad,
                               long downPayLoad)
    {
        this.mobileNumber = new Text(mobileNumber);
        this.upPayLoad = new LongWritable(upPayLoad);
        this.downPayLoad = new LongWritable(downPayLoad);
        this.totalPayLoad = new LongWritable(upPayLoad + downPayLoad);
    }

    @Override
    public int compareTo(MobileCountWritable other)
    {
        int result;
        if((result = this.totalPayLoad.compareTo(other.totalPayLoad)) != 0)
            return result;
        if((result = this.downPayLoad.compareTo(other.downPayLoad)) != 0)
            return result;
        if((result = this.upPayLoad.compareTo(other.upPayLoad)) != 0)
            return result;


        return 0;
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
        this.mobileNumber.write(out);
        this.upPayLoad.write(out);
        this.downPayLoad.write(out);
        this.totalPayLoad.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException
    {
        this.mobileNumber.readFields(in);
        this.upPayLoad.readFields(in);
        this.downPayLoad.readFields(in);
        this.totalPayLoad.readFields(in);
    }

    public String getMobileNumber()
    {
        return mobileNumber.toString();
    }

    public void setMobileNumber(String mobileNumber)
    {
        this.mobileNumber.set(mobileNumber);
    }

    public long getUpPayLoad()
    {
        return upPayLoad.get();
    }

    public void setUpPayLoad(long upPayLoad)
    {
        this.upPayLoad.set(upPayLoad);
    }

    public long getDownPayLoad()
    {
        return downPayLoad.get();
    }

    public void setDownPayLoad(long downPayLoad)
    {
        this.downPayLoad.set(downPayLoad);
    }
}
