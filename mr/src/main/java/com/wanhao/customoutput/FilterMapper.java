package com.wanhao.customoutput;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by LiuLiHao on 2020/10/12 15:57
 *
 * @author : LiuLiHao
 * 描述：
 */
public class FilterMapper extends Mapper<LongWritable, Text,Text, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        //直接输出 http://www.baidu.com
        context.write(value,NullWritable.get());
    }
}
