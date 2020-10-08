package com.wanhao.sort;

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
public class SortFlowSumDriver {

    public static void main(String[] args) throws Exception {
        args = new String[]{"D:/output1","D:/output3"};
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(SortFlowSumDriver.class);

        job.setMapperClass(SortFlowMapper.class);
        job.setReducerClass(SortFlowReducer.class);

        //输出类型
        job.setMapOutputKeyClass(SortFlowBean.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(SortFlowBean.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //自定义分区输出结果
        job.setPartitionerClass(SortFlowPartition.class);
        job.setNumReduceTasks(5);

        //处理小文件
        job.setInputFormatClass(CombineTextInputFormat.class);
        //虚拟切片大小设置
        CombineTextInputFormat.setMaxInputSplitSize(job, 1024 * 1024 * 128);

        System.out.println(job.waitForCompletion(true));
    }
}
