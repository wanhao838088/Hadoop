package com.wanhao.flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by LiuLiHao on 2020/10/6 16:29
 * @author : LiuLiHao
 * 描述：
 */
public class FlowSumMapper extends Mapper<LongWritable, Text,Text,FlowBean> {
    FlowBean v = new FlowBean();
    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String str = value.toString();
        /*
            1	13736230513	192.196.100.1	www.baidu.com	2481	24681	200
            2	13846544121	192.196.100.2			264	0	200
         */
        String[] arr = str.split("\t");

        v.setDownFlow(Long.parseLong(arr[arr.length-3]));
        v.setUpFlow(Long.parseLong(arr[arr.length-2]));
        k.set(arr[1]);

        context.write(k, v);
    }
}
