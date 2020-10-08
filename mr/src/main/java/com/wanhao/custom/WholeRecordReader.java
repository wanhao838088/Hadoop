package com.wanhao.custom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created by LiuLiHao on 2020/10/7 19:39
 *
 * @author : LiuLiHao
 * 描述：
 */
public class WholeRecordReader extends RecordReader<Text, BytesWritable> {

    FileSplit fileSplit;
    Configuration configuration;
    BytesWritable byteWritable = new BytesWritable();
    Text text = new Text();
    boolean flag = true;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        fileSplit = (FileSplit) split;
        configuration = context.getConfiguration();

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (flag){
            byte[] bys = new byte[(int) fileSplit.getLength()];

            Path path = fileSplit.getPath();
            FileSystem fs = path.getFileSystem(configuration);
            FSDataInputStream inputStream = fs.open(path);
            //保存到数组
            IOUtils.readFully(inputStream, bys, 0, bys.length);

            byteWritable.set(bys,0,bys.length);
            text.set(path.toString());
            IOUtils.closeStream(inputStream);
            flag = !flag;
            return true;
        }
        return false;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return text;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return byteWritable;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
