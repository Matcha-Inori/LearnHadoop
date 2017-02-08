package com.matcha.learn.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by Matcha on 2017/2/2.
 */
@RunWith(Parameterized.class)
public class TestMapFile
{
    private Logger logger = Logger.getLogger(TestMapFile.class);

    private String fsURI;

    private List<DataInfo> data;

    public TestMapFile(String fsURI, List<DataInfo> data)
    {
        this.fsURI = fsURI;
        this.data = data;
    }

    @Ignore
    @Test
    public void testWrite() throws Exception
    {
        Configuration configuration = new Configuration();
        String ticketCachePath =
                configuration.get(CommonConfigurationKeys.KERBEROS_TICKET_CACHE_PATH);
        UserGroupInformation ugi =
                UserGroupInformation.getBestUGI(ticketCachePath, "matcha");
        ugi.doAs(new PrivilegedExceptionAction<Void>()
        {
            @Override
            public Void run() throws Exception
            {
                URI mapFileURI = URI.create(fsURI + "/matcha/test/mapFile/test01");
                Path mapFilePath = new Path(mapFileURI);
                try(
                        MapFile.Writer mapWriter = new MapFile.Writer(
                                configuration,
                                mapFilePath,
                                MapFile.Writer.keyClass(DataInfo.class),
                                MapFile.Writer.valueClass(LongWritable.class)
                        )
                )
                {
                    LongWritable indexWritable = new LongWritable();
                    DataInfo dataInfo;
                    for(int index = 0;index < TestMapFile.this.data.size();index++)
                    {
                        dataInfo = TestMapFile.this.data.get(index);
                        indexWritable.set(index);
                        mapWriter.append(dataInfo, indexWritable);
                    }
                }
                return null;
            }
        });
    }

    @Test
    public void testRead() throws Exception
    {
        Configuration configuration = new Configuration();
        String ticketCachePath =
                configuration.get(CommonConfigurationKeys.KERBEROS_TICKET_CACHE_PATH);
        UserGroupInformation ugi =
                UserGroupInformation.getBestUGI(ticketCachePath, "matcha");
        ugi.doAs(new PrivilegedExceptionAction<Void>()
        {
            @Override
            public Void run() throws Exception
            {
                URI mapFileURI = URI.create(fsURI + "/matcha/test/mapFile/test01");
                Path mapFilePath = new Path(mapFileURI);
                try(
                        MapFile.Reader reader = new MapFile.Reader(
                                mapFilePath,
                                configuration
                        )
                )
                {
                    System.out.println("=====ergodic=====");
                    DataInfo dataInfo = new DataInfo();
                    LongWritable longWritable = new LongWritable();
                    while(reader.next(dataInfo, longWritable))
                        System.out.println(String.format("%s\t\t = %d", dataInfo.toString(), longWritable.get()));
                    System.out.println("=====ergodic=====");
                    dataInfo = new DataInfo(24L, "LiSA");
                    longWritable = (LongWritable) reader.get(dataInfo, longWritable);
                    if(longWritable != null)
                        System.out.println(String.format("%s\t\t = %d", dataInfo.toString(), longWritable.get()));
                }
                return null;
            }
        });
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data()
    {
        List<DataInfo> dataInfos = new ArrayList<>();

        List<Object[]> params = new ArrayList<>();

        //这里的顺序不能乱，如果不是按照从小到大的顺序排列，会报错
        dataInfos.add(new DataInfo(23L, "Riven"));
        dataInfos.add(new DataInfo(23L, "Tom"));
        dataInfos.add(new DataInfo(24L, "LiSA"));
        dataInfos.add(new DataInfo(25L, "Mail"));
        dataInfos.add(new DataInfo(26L, "Maven"));

        params.add(new Object[]{"hdfs://centos01:9000", dataInfos});
        return params;
    }

    private static class DataInfo implements WritableComparable<DataInfo>
    {
        private LongWritable index;
        private Text info;

        public DataInfo()
        {
            this.index = new LongWritable();
            this.info = new Text();
        }

        public DataInfo(long index, String info)
        {
            this.index = new LongWritable(index);
            this.info = new Text(info);
        }

        @Override
        public int compareTo(DataInfo otherDataInfo)
        {
            if(otherDataInfo == null)
                return 1;

            int result = (result = index.compareTo(otherDataInfo.index)) == 0 ?
                    this.info.compareTo(otherDataInfo.info) :
                    result;
            return result;
        }

        @Override
        public void write(DataOutput out) throws IOException
        {
            index.write(out);
            info.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException
        {
            index.readFields(in);
            info.readFields(in);
        }

        public void set(DataInfo dataInfo)
        {
            index.set(dataInfo.index.get());
            info.set(dataInfo.info.toString());
        }

        public void set(long index, String info)
        {
            this.index.set(index);
            this.info.set(info);
        }

        @Override
        public String toString()
        {
            return String.format("%d\t\t - %s\t\t", index.get(), info.toString());
        }
    }

    private static class DataInfoComparator extends WritableComparator
    {
        public DataInfoComparator()
        {
            super(DataInfo.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
        {
            WritableComparator longComparator = WritableComparator.get(LongWritable.class);
            WritableComparator textComparator = WritableComparator.get(Text.class);
            int result = (result = longComparator.compare(b1, s1, 8, b2, s2, 8)) == 0 ?
                    textComparator.compare(b1, s1 + 8, l1 - 8, b2, s2 + 8, l2 - 8) :
                    result;
            return result;
        }
    }

    static
    {
        WritableComparator.define(DataInfo.class, new DataInfoComparator());
    }
}
