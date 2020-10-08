package com.wanhao.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by LiuLiHao on 2020/10/8 19:35
 *
 * @author : LiuLiHao
 * 描述：
 */
public class SortFlowReducer extends Reducer<SortFlowBean, Text,Text,SortFlowBean> {
    @Override
    protected void reduce(SortFlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(value,key);
        }
    }
}
