package com.wanhao.job;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created by LiuLiHao on 2020/10/19 9:18
 *
 * @author : LiuLiHao
 * 描述：
 */
public class FirstJobMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    String name = null;
    Text k = new Text();
    IntWritable v = new IntWritable(1);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //获取文件名
        FileSplit fileSplit  = (FileSplit) context.getInputSplit();
        name = fileSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        //输入内容 aa bb cc
        String line = value.toString();
        String[] array = line.split(" ");
        for (String s : array) {
            k.set(s + "--" + name);
            context.write(k,v);
        }
        //写出内容
        // aa--a.txt bb--b.txt

    }
}
