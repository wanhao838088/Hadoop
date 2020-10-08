package com.wanhao.custom;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by LiuLiHao on 2020/10/7 20:08
 *
 * @author : LiuLiHao
 * 描述：
 */
public class SequenceFileMapper extends Mapper<Text, BytesWritable,Text, BytesWritable> {

    @Override
    protected void map(Text key, BytesWritable value, Context context)
            throws IOException, InterruptedException {
        context.write(key,value);
    }
}
