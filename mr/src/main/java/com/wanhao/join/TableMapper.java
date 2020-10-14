package com.wanhao.join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created by LiuLiHao on 2020/10/14 9:02
 *
 * @author : LiuLiHao
 * 描述：
 */
public class TableMapper extends Mapper<LongWritable, Text,Text, TableBean> {
    FileSplit fileSplit;
    Text k = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //获取文件信息
        fileSplit = (FileSplit) context.getInputSplit();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String name = fileSplit.getPath().getName();
        String content = value.toString();
        String[] ordInfos = content.split("\t");

        if(name.contains("order")){
            //订单表
            //        id	pid	 amount
            //        1001	01	 1
            TableBean tableBean = new TableBean();
            tableBean.setOrder_id(ordInfos[0]);
            tableBean.setP_id(ordInfos[1]);
            tableBean.setAmount(Integer.parseInt(ordInfos[2]));
            tableBean.setFlag("order");
            tableBean.setPname("");
            k.set(ordInfos[1]);

            context.write(k,tableBean);
        }else {
            //产品表
            //01	小米
            TableBean tableBean = new TableBean();
            tableBean.setP_id(ordInfos[0]);
            tableBean.setPname(ordInfos[1]);
            tableBean.setOrder_id("");
            tableBean.setAmount(0);
            tableBean.setFlag("product");

            k.set(ordInfos[0]);

            context.write(k,tableBean);
        }
    }
}
