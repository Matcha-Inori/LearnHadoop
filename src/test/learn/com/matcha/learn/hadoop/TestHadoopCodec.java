package com.matcha.learn.hadoop;

import io.netty.buffer.ByteBuf;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.GzipCodec;
import org.junit.Ignore;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 * Created by Matcha on 2016/11/11.
 */
public class TestHadoopCodec
{
    @Ignore
    @Test
    public void testHadoopCodec() throws IOException, URISyntaxException
    {
        URI testGZip = new File("").toURI().resolve("src/main/resources/output/testGZip.gzip");
        URL testCodecURL = Thread.currentThread().getContextClassLoader().getResource("data/testCodecFile.txt");
        CompressionCodec compressionCodec = new GzipCodec();
        try(
                FileOutputStream fileOutputStream = new FileOutputStream(new File(testGZip));
                CompressionOutputStream compressionOutputStream = compressionCodec.createOutputStream(fileOutputStream);
                WritableByteChannel writableByteChannel = Channels.newChannel(compressionOutputStream);
                RandomAccessFile randomAccessFile = new RandomAccessFile(new File(testCodecURL.toURI()), "rw");
                FileChannel fileChannel = randomAccessFile.getChannel()
        )
        {
            fileChannel.transferTo(0, fileChannel.size(), writableByteChannel);
        }
        catch (IOException | URISyntaxException e)
        {
            e.printStackTrace();
            throw e;
        }
    }

    @Test
    public void testHaddopCodec2()
    {
        URL gzipFileURL = Thread.currentThread().getContextClassLoader().getResource("output/testGZip.gzip");
        CompressionCodec compressionCodec = new GzipCodec();
        ((Configurable) compressionCodec).setConf(new Configuration());
        try(
                FileInputStream fileInputStream = new FileInputStream(new File(gzipFileURL.toURI()));
                CompressionInputStream compressionInputStream = compressionCodec.createInputStream(fileInputStream);
                ReadableByteChannel readableByteChannel = Channels.newChannel(compressionInputStream)
        )
        {
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(1024);
            readableByteChannel.read(byteBuffer);
            byteBuffer.flip();
            int remaining = byteBuffer.remaining();
            byte[] bytes = new byte[remaining];
            byteBuffer.get(bytes);
            System.out.println(new String(bytes));
        }
        catch (URISyntaxException e)
        {
            e.printStackTrace();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }
}
