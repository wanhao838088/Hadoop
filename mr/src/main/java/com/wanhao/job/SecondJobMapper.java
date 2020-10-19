package com.wanhao.job;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by LiuLiHao on 2020/10/19 9:30
 *
 * @author : LiuLiHao
 * 描述：
 */
public class SecondJobMapper extends Mapper<LongWritable, Text,Text,Text> {
    Text k = new Text();
    Text v = new Text();


    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        //输入内容 aaa--a.txt	3
        String line = value.toString();
        String[] array = line.split("--");
        k.set(array[0]);
        v.set(array[1]);

        context.write(k,v);
    }
}
