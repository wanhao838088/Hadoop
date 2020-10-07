package com.wanhao.nlineformat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by LiuLiHao on 2020/10/7 11:51
 *
 * @author : LiuLiHao
 * 描述：
 */
public class NLineMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    Text k = new Text();
    IntWritable v = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] arr = value.toString().split(" ");

        for (String s : arr) {
            k.set(s);
            context.write(k,v);
        }
    }
}
