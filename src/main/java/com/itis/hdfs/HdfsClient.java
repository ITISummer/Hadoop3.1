package com.itis.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @ClassName HdfsClient
 * @Author LCX
 * @Date 2021 2021-09-17 11:14 p.m.
 * @Version 1.0
 **/

public class HdfsClient {

    private FileSystem fs = null;

    @Before
    public void init() throws IOException, URISyntaxException, InterruptedException {
        // 连接的集群 nn 地址
        URI uri = new URI("hdfs://hadoop102:8020");
        // 创建一个配置文件
        Configuration configuration = new Configuration();
        // 访问的用户
        String user = "itis";
        // 1 获取到了客户端对象
        fs = FileSystem.get(uri, configuration, user);

    }

    /**
     * 关闭资源
     */
    @After
    public void close() throws IOException {
        fs.close();
    }

    @Test
    public void testMkdir() throws IOException, URISyntaxException, InterruptedException {
        // 创建一个文件夹
        fs.mkdirs(new Path("/xiyou/huaguoshan"));
    }

    @Test
    public void testPut() throws IOException {
        fs.copyFromLocalFile(false,false,new Path("D:/CodingsAndWorkplaces/IntellijIdeaWorkplace/Hadoop3.1/src/main/resources/sunwukong.txt"),new Path("hdfs://hadoop102/xiyou/huaguoshan"));
    }

    /**
     * 文件下载
     */
    @Test
    public void testGet() throws IOException {
       fs.copyToLocalFile(false,new Path("hdfs://hadoop102/xiyou/huaguoshan"),new Path("D:\\HadoopTest"),false);
    }

    /**
     * 文件删除
     */
    @Test
    public void testRm() throws IOException{
        // 非递归删除
        fs.delete(new Path("/xiyou"),false);
        // 递归删除
        fs.delete(new Path("/sanguo"),true);
    }

    /**
     * 文件的更名和移动
     */
    @Test
    public void testMv() throws IOException {
        // 源文件路径，目标文件夹
        fs.rename(new Path("/input/word.txt"),new Path("/input/ss.txt"));
    }
}
