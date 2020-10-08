package com.wanhao.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by LiuLiHao on 2020/10/8 19:28
 *
 * @author : LiuLiHao
 * 描述：
 */
public class SortFlowMapper extends Mapper<LongWritable, Text,SortFlowBean,Text> {

    SortFlowBean bean = new SortFlowBean();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //13630577991	690	6960	7650
        String[] arr = value.toString().split("\t");
        if (arr.length<3){
            return;
        }
        String val = arr[0];
        String up = arr[1];
        String down = arr[2];
        String sum = arr[3];

        bean.setUpFlow(Long.parseLong(up));
        bean.setDownFlow(Long.parseLong(down));
        bean.setSumFlow(Long.parseLong(sum));
        v.set(val);
        //写出
        context.write(bean,v);
    }
}
