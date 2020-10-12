package com.wanhao.customoutput;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by LiuLiHao on 2020/10/12 16:04
 * @author : LiuLiHao
 * 描述：自定义OutputFormat
 */
public class FilterOutputFormat extends FileOutputFormat<Text, NullWritable> {

    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext job)
            throws IOException, InterruptedException {
        return new OutWriter(job);
    }

    static class OutWriter extends RecordWriter<Text, NullWritable>{
        FSDataOutputStream fsLog1;
        FSDataOutputStream fsLog2;

        public OutWriter(TaskAttemptContext job) {
            FileSystem fileSystem;
            try {
                fileSystem = FileSystem.get(job.getConfiguration());
                Path log1 = new Path("d:/log1.txt");
                Path log2 = new Path("d:/log2.txt");

                //创建流
                fsLog1 = fileSystem.create(log1);
                fsLog2 = fileSystem.create(log2);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void write(Text key, NullWritable value) throws IOException, InterruptedException {
            //写出到2个文件
            if (key.toString().contains("baidu")){
                fsLog1.write(key.toString().getBytes());
            }else {
                fsLog2.write(key.toString().getBytes());
            }
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            IOUtils.closeStream(fsLog1);
            IOUtils.closeStream(fsLog2);
        }
    }

}
