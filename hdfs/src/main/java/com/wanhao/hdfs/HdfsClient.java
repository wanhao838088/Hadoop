package com.wanhao.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

/**
 * Created by LiuLiHao on 2020/9/27 0027 上午 09:56
 *
 * @author : LiuLiHao
 * 描述： hdfs系列操作
 */
public class HdfsClient {
    Configuration configuration = new Configuration();
    FileSystem fileSystem = null;

    @Before
    public void before() throws Exception {
        fileSystem = FileSystem.get(new URI("hdfs://192.168.8.131:9000"), configuration, "hadoop");
    }

    @After
    public void after() {
        try {
            fileSystem.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            fileSystem = null;
        }
    }

    /**
     * 创建文件夹
     *
     * @throws Exception
     */
    @Test
    public void testMkdir() throws Exception {
        fileSystem.mkdirs(new Path("/from_java"));
    }

    /**
     * 上传
     *
     * @throws Exception
     */
    @Test
    public void copyFromLocal() throws Exception {
        configuration.set("dfs.replication", "2");
        fileSystem.copyFromLocalFile(new Path("G:/fanmian.jpg"), new Path("/fanmian.jpg"));
    }

    /**
     * 下载
     *
     * @throws Exception
     */
    @Test
    public void copyToLocalFile() throws Exception {
        fileSystem.copyToLocalFile(new Path("/fanmian.jpg"), new Path("G:/down.jpg"));
    }

    /**
     * 删除
     *
     * @throws Exception
     */
    @Test
    public void delete() throws Exception {
        fileSystem.delete(new Path("/fanmian.jpg"), true);
    }

    /**
     * 重命名
     *
     * @throws Exception
     */
    @Test
    public void rename() throws Exception {
        fileSystem.rename(new Path("/README.txt"), new Path("/read.txt"));
    }

    /**
     * 文件列表查看
     *
     * @throws Exception
     */
    @Test
    public void listFiles() throws Exception {
        RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(new Path("/"), true);

        while (files.hasNext()) {
            LocatedFileStatus fileStatus = files.next();
            System.out.println(fileStatus.getPath().getName());
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getGroup());

            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                //存储机器
                System.out.println(Arrays.toString(blockLocation.getHosts()));
            }

            System.out.println("---------");
        }
    }

    /**
     * 是否文件
     *
     * @throws Exception
     */
    @Test
    public void isFile() throws Exception {
        RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(new Path("/"), true);

        while (files.hasNext()) {
            LocatedFileStatus fileStatus = files.next();
            if (fileStatus.isFile()) {
                System.out.println("文件" + fileStatus.getPath().getName());
            } else {
                System.out.println("文件夹" + fileStatus.getPath().getName());
            }
        }
    }

    /**
     * 上传文件到hdfs
     */
    @Test
    public void putFileToHDFS() throws Exception {
        FileInputStream fis = new FileInputStream(new File("G:/settings.jar"));
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/ioPut.jar"));
        IOUtils.copyBytes(fis, fsDataOutputStream, configuration);
        IOUtils.closeStream(fsDataOutputStream);
        IOUtils.closeStream(fis);
    }

    /**
     * 从hdfs下载文件
     *
     * @throws Exception
     */
    @Test
    public void getFileFromHDFS() throws Exception {
        FSDataInputStream inputStream = fileSystem.open(new Path("/ioPut.jar"));
        FileOutputStream fileOutputStream = new FileOutputStream(new File("G:/down.jar"));
        IOUtils.copyBytes(inputStream, fileOutputStream, configuration);
        //释放
        IOUtils.closeStream(fileOutputStream);
        IOUtils.closeStream(inputStream);
    }

    /**
     * 指定位置读取文件
     */
    @Test
    public void readFileSeek1() throws Exception {
        Path path = new Path("/ioPut.jar");

        FSDataInputStream inputStream = fileSystem.open(path);
        //文件总长度
        int limit = inputStream.available();

        //设置读取开始位置
        int from = 1024 * 1024 * 128;
        int to = 1024 * 1024 * 256;

        inputStream.seek(from);

        FileOutputStream fileOutputStream = new FileOutputStream(new File("G:/out1.zip"));

        byte[] buf = new byte[1024];

        for (int i = 0; i < to && i < limit; i++) {
            inputStream.read(buf);
            fileOutputStream.write(buf);
        }
        //释放
        IOUtils.closeStream(fileOutputStream);
        IOUtils.closeStream(inputStream);
    }
}
