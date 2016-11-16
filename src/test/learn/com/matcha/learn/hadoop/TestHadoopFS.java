package com.matcha.learn.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 * Created by Matcha on 2016/11/9.
 */
@RunWith(Parameterized.class)
public class TestHadoopFS
{
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
            fileSystem = FileSystem.get(uri, configuration);
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
    }

    @Test
    public void testLoadFile() throws IOException, URISyntaxException
    {
        Path newFilePath = new Path("/matcha/test/fistFile.txt");
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
        //这个地方的链接其实涉及到hdfs的登录，不过先暂时关闭hdfs的权限认证吧
        Path newFilePath = new Path("/matcha/test/fistFile.txt");
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

    @Parameterized.Parameters
    public static Object[][] provideParams()
    {
        return new Object[][]{
                {
                        "hdfs://192.168.56.101:9000/"
                }
        };
    }
}
