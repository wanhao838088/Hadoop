package com.wanhao;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by LiuLiHao on 2020/10/6 12:16
 * @author : LiuLiHao
 * 描述：
 */
public class WordCountReducer extends Reducer<Text, IntWritable,Text, IntWritable> {

    IntWritable val = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int sum = 0;
        for (IntWritable count : values) {
            sum += count.get();
        }
        val.set(sum);
        context.write(key,val);
    }
}
