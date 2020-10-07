package com.wanhao.flow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by LiuLiHao on 2020/10/6 16:42
 *
 * @author : LiuLiHao
 * 描述：
 */
public class FlowSumDriver {

    public static void main(String[] args) throws Exception {
        args = new String[]{"D:/input","D:/output"};
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(FlowSumDriver.class);

        job.setMapperClass(FlowSumMapper.class);
        job.setReducerClass(FlowSumReducer.class);

        //输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //处理小文件
        job.setInputFormatClass(CombineTextInputFormat.class);
        //虚拟切片大小设置
        CombineTextInputFormat.setMaxInputSplitSize(job, 1024 * 1024 * 128);

        System.out.println(job.waitForCompletion(true));
    }
}
