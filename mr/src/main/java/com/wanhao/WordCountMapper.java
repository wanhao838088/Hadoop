package com.wanhao;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by LiuLiHao on 2020/10/6 12:10
 * @author : LiuLiHao
 * 描述：
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text,IntWritable> {
    Text text = new Text();
    IntWritable intWritable = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String s = value.toString();
        String[] arr = s.split(" ");
        for (String cur : arr) {
            text.set(cur);
            context.write(text,intWritable);
        }

    }
}
