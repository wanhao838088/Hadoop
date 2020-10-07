package com.wanhao.keyvalueformat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by LiuLiHao on 2020/10/7 11:27
 * @author : LiuLiHao
 * 描述：
 */
public class KVMapper extends Mapper<Text,Text,Text, IntWritable> {
    IntWritable v = new IntWritable(1);

    @Override
    protected void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {

        context.write(key,v);
    }
}
