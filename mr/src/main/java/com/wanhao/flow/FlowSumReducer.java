package com.wanhao.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by LiuLiHao on 2020/10/6 16:39
 *
 * @author : LiuLiHao
 * 描述：
 */
public class FlowSumReducer extends Reducer<Text,FlowBean,Text,FlowBean> {

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context)
            throws IOException, InterruptedException {

        long sumUp = 0;
        long sumDown = 0;

        //汇总
        for (FlowBean flowBean : values) {
            sumUp += flowBean.getUpFlow();
            sumDown += flowBean.getDownFlow();
        }

        FlowBean result = new FlowBean(sumUp,sumDown);
        context.write(key,result);
    }
}
