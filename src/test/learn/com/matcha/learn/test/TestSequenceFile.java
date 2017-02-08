package com.matcha.learn.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.*;

/**
 * Created by Matcha on 2017/2/1.
 */
@RunWith(Parameterized.class)
public class TestSequenceFile
{
    private Logger logger = Logger.getLogger(TestSequenceFile.class);

    private String fsURI;

    private SortedMap<Long, String> data;

    public TestSequenceFile(String fsURI, SortedMap<Long, String> data)
    {
        this.fsURI = fsURI;
        this.data = data;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data()
    {
        List<Object[]> params = new ArrayList<>();

        SortedMap<Long, String> data = new TreeMap<>();
        data.put(23L, "Riven");
        data.put(24L, "Tom");
        data.put(25L, "Maven");
        data.put(26L, "Mail");
        data.put(100L, "LiSA");

        params.add(new Object[]{"hdfs://centos01:9000", data});
        return params;
    }

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
                URI sequenceFileURI = URI.create(TestSequenceFile.this.fsURI + "/matcha/test/sequenceFile/test01");
                Path sequenceFilePath = new Path(sequenceFileURI);
                try(
                        SequenceFile.Writer writer = SequenceFile.createWriter(
                                configuration,
                                SequenceFile.Writer.file(sequenceFilePath),
                                SequenceFile.Writer.keyClass(LongWritable.class),
                                SequenceFile.Writer.valueClass(Text.class)
                        )
                )
                {
                    Set<Map.Entry<Long, String>> entrySet = data.entrySet();
                    Long key;
                    String value;
                    LongWritable longWritable = new LongWritable();
                    Text text = new Text();
                    for(Map.Entry<Long, String> entry : entrySet)
                    {
                        key = entry.getKey();
                        value = entry.getValue();
                        longWritable.set(key);
                        text.set(value);
                        writer.append(longWritable, text);
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
                URI sequenceFileURI = URI.create(TestSequenceFile.this.fsURI + "/matcha/test/sequenceFile/test01");
                Path sequenceFilePath = new Path(sequenceFileURI);
                try(
                        SequenceFile.Reader reader = new SequenceFile.Reader(
                                new Configuration(),
                                SequenceFile.Reader.file(sequenceFilePath),
                                SequenceFile.Reader.bufferSize(1024)
                        )
                )
                {
                    LongWritable longWritable = new LongWritable();
                    Text text = new Text();
                    while(reader.next(longWritable, text))
                        System.out.println(String.format("%d - %s", longWritable.get(), text.toString()));
                }
                return null;
            }
        });
    }
}
