package com.wanhao.join;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by LiuLiHao on 2020/10/14 9:21
 *
 * @author : LiuLiHao
 * 描述：
 */
public class TableReducer extends Reducer<Text,TableBean,TableBean, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context)
            throws IOException, InterruptedException {

        List<TableBean> tables = new ArrayList<>();
        TableBean pdBean = new TableBean();

        for (TableBean tableBean : values) {
            String flag = tableBean.getFlag();
            if (flag.equals("order")){
                //订单
                TableBean temp = new TableBean();
                try {
                    BeanUtils.copyProperties(temp,tableBean);
                    tables.add(temp);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }else {

                try {
                    BeanUtils.copyProperties(pdBean,tableBean);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        // 表的拼接
        for(TableBean bean: tables){
            bean.setPname (pdBean.getPname());

            context.write(bean, NullWritable.get());
        }

    }
}
