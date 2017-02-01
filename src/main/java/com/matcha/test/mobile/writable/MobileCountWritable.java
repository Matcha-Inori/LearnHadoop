package com.matcha.test.mobile.writable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

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

    public MobileCountWritable()
    {
        this(null, 0, 0);
    }

    public MobileCountWritable(String mobileNumber)
    {
        this(mobileNumber, 0, 0);
    }

    public MobileCountWritable(Text mobileNumber)
    {
        this(mobileNumber.toString(), 0, 0);
    }

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
        this.mobileNumber = mobileNumber == null ? new Text() : new Text(mobileNumber);
        this.upPayLoad = new LongWritable(upPayLoad);
        this.downPayLoad = new LongWritable(downPayLoad);
        this.totalPayLoad = new LongWritable(upPayLoad + downPayLoad);
    }

    public void add(MobileCountWritable other)
    {
        this.upPayLoad.set(this.upPayLoad.get() + other.upPayLoad.get());
        this.downPayLoad.set(this.downPayLoad.get() + other.downPayLoad.get());
        this.totalPayLoad.set(this.totalPayLoad.get() + other.totalPayLoad.get());
    }

    public void set(Text mobileNumber, long upPayLoad, long downPayLoad)
    {
        this.mobileNumber.set(mobileNumber.toString());
        this.upPayLoad.set(upPayLoad);
        this.downPayLoad.set(downPayLoad);
        this.totalPayLoad.set(upPayLoad + downPayLoad);
    }

    public void reCompute()
    {
        this.totalPayLoad.set(this.upPayLoad.get() + this.downPayLoad.get());
    }

    @Override
    public int compareTo(MobileCountWritable other)
    {
        return this.mobileNumber.compareTo(other.mobileNumber);
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

    @Override
    public String toString()
    {
        StringBuffer stringBuffer = new StringBuffer();
        stringBuffer.append(this.upPayLoad).append("\t")
                .append(this.downPayLoad).append("\t")
                .append(this.totalPayLoad);
        return stringBuffer.toString();
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
        reCompute();
    }

    public long getDownPayLoad()
    {
        return downPayLoad.get();
    }

    public void setDownPayLoad(long downPayLoad)
    {
        this.downPayLoad.set(downPayLoad);
        reCompute();
    }

    public long getTotalPayLoad()
    {
        return totalPayLoad.get();
    }

    public static class MobileCountWritableComparator extends WritableComparator
    {
        @Override
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2)
        {
            WritableComparator textWritableComparator = WritableComparator.get(Text.class);
            return textWritableComparator.compare(b1, s1, l1, b2, s2, l2);
        }
    }

    static
    {
        WritableComparator.define(MobileCountWritable.class, new MobileCountWritableComparator());
    }
}
