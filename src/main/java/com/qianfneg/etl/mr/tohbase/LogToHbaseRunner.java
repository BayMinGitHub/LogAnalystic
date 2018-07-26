package com.qianfneg.etl.mr.tohbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.log4j.Logger;

/**
 * @Description: 驱动类
 * Author by BayMin, Date on 2018/7/26.
 */
public class LogToHbaseRunner implements Tool {
    private static final Logger logger = Logger.getLogger(LogToHbaseRunner.class);
    Configuration conf = null;

    @Override
    public void setConf(Configuration conf) {
        this.conf = HBaseConfiguration.create();
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    /**
     * yarn jar /xxx/xxx.jar com.qianfneg.etl.mr.tohbase.LogToHbaseRunner -d 2018-7-26
     */
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.conf;
        // 设置处理的参数
        this.setArgs(args, conf);
        // 判断hbase的表是否存在
        this.HbaseTableExists(conf);
        // 获取job
        Job job = Job.getInstance(conf, "to hbase etl");
        job.setJarByClass(LogToHbaseRunner.class);

        // 设置map端属性
        job.setMapperClass(LogToHbaseMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Put.class);

        return 0;
    }


    /**
     * 参数处理
     */
    private void setArgs(String[] args, Configuration conf) {
    }

    /**
     * 判断hbase表是否存在,不存在则创建
     */
    private void HbaseTableExists(Configuration conf) {
    }
}
