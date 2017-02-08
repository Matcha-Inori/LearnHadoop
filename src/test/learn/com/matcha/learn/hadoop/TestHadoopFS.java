package com.matcha.learn.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;

/**
 * Created by Matcha on 2016/11/9.
 */
@RunWith(Parameterized.class)
public class TestHadoopFS
{
    private Logger logger = Logger.getLogger(TestHadoopFS.class);

    private String fsURI;
    private FileSystem fileSystem;

    public TestHadoopFS(String fsURI)
    {
        this.fsURI = fsURI;
    }

    @Before
    public void setUp() throws Exception
    {
        try
        {
            URI uri = URI.create(fsURI);
            Configuration configuration = new Configuration();
            fileSystem = FileSystem.get(uri, configuration, "matcha");
            DOMConfigurator.configure("properties/log4jConfig.xml");
            logger.info("TestHadoopFS test start!");
        }
        catch (IOException e)
        {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @After
    public void tearDown() throws Exception
    {
        if(fileSystem != null)
            fileSystem.close();
        logger.info("TestHadoopFS test finish!");
    }

    @Test
    public void testLoadFile() throws IOException, URISyntaxException
    {
        Path newFilePath = new Path("/matcha/test/firstFile.txt");
        URI firstLocalFileURI = new File("").toURI().resolve("src/main/resources/output/firstLocalFile.txt");
        try(
                FSDataInputStream fsDataInputStream = fileSystem.open(newFilePath);
                ReadableByteChannel readableByteChannel = Channels.newChannel(fsDataInputStream);
                RandomAccessFile randomAccessFile = new RandomAccessFile(new File(firstLocalFileURI), "rw");
                FileChannel fileChannel = randomAccessFile.getChannel()
        )
        {
            fileChannel.transferFrom(readableByteChannel, 0, fsDataInputStream.available());
        }
        catch (IOException e)
        {
            e.printStackTrace();
            throw e;
        }
    }

    @Ignore
    @Test
    public void testCreateFile() throws IOException, URISyntaxException
    {
        //这个地方的权链接其实涉及到hdfs的登录，不过先暂时关闭hdfs的限认证吧
        Path newFilePath = new Path("/matcha/test/firstFile.txt");
        FsPermission fsPermission = new FsPermission(FsAction.ALL, FsAction.READ, FsAction.READ);
        URL localFileURL = Thread.currentThread().getContextClassLoader().getResource("data/testFile.txt");
        try(
                FSDataOutputStream fsDataOutputStream = fileSystem.create(newFilePath);
                WritableByteChannel writableByteChannel = Channels.newChannel(fsDataOutputStream);
                RandomAccessFile randomAccessFile = new RandomAccessFile(new File(localFileURL.toURI()), "r");
                FileChannel fileChannel = randomAccessFile.getChannel();
        )
        {
            fileChannel.transferTo(0, fileChannel.size(), writableByteChannel);
            fileSystem.setPermission(newFilePath, fsPermission);
        }
        catch (IOException e)
        {
            e.printStackTrace();
            throw e;
        }
        catch (URISyntaxException e)
        {
            e.printStackTrace();
            throw e;
        }
    }

    @Test
    public void testUGI() throws Exception
    {
        if(fileSystem != null)
            fileSystem.close();
        ClassLoader classLoader = this.getClass().getClassLoader();
        URL testConfigURL = classLoader.getResource("testConfig.xml");
        Configuration configuration = new Configuration();
        configuration.addResource(testConfigURL);
        Path testFilePath = new Path("/usr/abc.txt");
        try(
                FileSystem fileSystem = FileSystem.get(URI.create(fsURI), configuration);
                FSDataOutputStream outputStream = fileSystem.create(testFilePath)
        )
        {
            outputStream.writeChars("abc.txt\n");
            outputStream.writeChars("abc.txt\n");
            outputStream.writeChars("abc.txt");
            outputStream.hsync();
            FsPermission fsPermission = new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE);
            fileSystem.setPermission(testFilePath, fsPermission);
        }

        URL otherTestConfigURL = classLoader.getResource("otherTestConfig.xml");
        configuration.addResource(otherTestConfigURL);
        try(
                FileSystem fileSystem = FileSystem.get(URI.create(this.fsURI), configuration);
                FSDataInputStream inputStream = fileSystem.open(testFilePath);
                ReadableByteChannel channel = Channels.newChannel(inputStream)
        )
        {
            char[] result = new char[0];
            char[] chars = null;
            int remaining;
            int index;
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(1024);
            CharBuffer charBuffer;
            while(channel.read(byteBuffer) >= 0)
            {
                byteBuffer.flip();
                if(!byteBuffer.hasRemaining())
                    continue;
                charBuffer = byteBuffer.asCharBuffer();
                remaining = charBuffer.remaining();
                if(chars == null || chars.length < remaining)
                    chars = new char[remaining];
                charBuffer.get(chars);
                index = result.length;
                result = Arrays.copyOf(result, index + remaining);
                System.arraycopy(chars, 0, result, index, remaining);
                byteBuffer.clear();
            }
            String resultStr = new String(result);
            System.out.println(resultStr);
        }
    }

    @Parameterized.Parameters
    public static Object[][] provideParams()
    {
        return new Object[][]{
                {
                        "hdfs://centos01:9000/"
                }
        };
    }
}
