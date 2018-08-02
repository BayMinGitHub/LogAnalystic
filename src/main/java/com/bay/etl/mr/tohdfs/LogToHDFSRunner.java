package com.bay.etl.mr.tohdfs;

import com.bay.common.GlobalConstants;
import com.bay.util.TimeUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @Description: 写入到HDFS的Runner类
 * Author by BayMin, Date on 2018/7/27.
 */
public class LogToHDFSRunner implements Tool {
    private static final Logger logger = Logger.getLogger(LogToHDFSRunner.class);
    Configuration conf = null;

    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(), new LogToHDFSRunner(), args);
        } catch (Exception e) {
            logger.error("运行ETL TO HDFS异常.", e);
        }
    }

    @Override
    public void setConf(Configuration conf) {
        // 在写入HDFS中时也可以用,因为都是Configuration对象
        this.conf = HBaseConfiguration.create();
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.conf;
        // 设置处理的参数
        this.setArgs(args, conf);
        // 获取job
        Job job = Job.getInstance(conf, "TO HDFS ETL");
        job.setJarByClass(LogToHDFSRunner.class);

        // 设置map端属性
        job.setMapperClass(LogToHDFSMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(LogDataWritable.class);
        // 初始化reduce
        job.setNumReduceTasks(0);

        // 设置map阶段的输入路径
        this.setInputPath(job);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * 参数处理
     * 将接收到的日期存储在conf中,以供后续使用
     * 如果没有传递日期,则默认使用昨天的日期
     */
    private void setArgs(String[] args, Configuration conf) {
        String date = null;
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-d")) {
                if (i + 1 < args.length) {
                    date = args[i + 1];
                    break;
                }
            }
        }
        // 如果代码到这里,date还是null,默认用昨天的时间
        if (date == null)
            date = TimeUtil.getYesterdayDate();
        // 然后将date设置到时间conf中
        conf.set(GlobalConstants.RUNNING_DATE, date);
    }

    /**
     * 设置路径
     */
    private void setInputPath(Job job) {
        FileSystem fs = null;
        String date = job.getConfiguration().get(GlobalConstants.RUNNING_DATE);
        String[] fields = date.split("-");
        Path inputPath = new Path("/flume/events/" + fields[1] + "-" + fields[2]);
        Path outputPath = new Path("/ods/month" + fields[1] + "/day" + fields[2]);
        try {
            fs = FileSystem.get(conf);
            if (fs.exists(inputPath))
                FileInputFormat.addInputPath(job, inputPath);
            else {
                logger.warn("路径为:" + inputPath.toString());
                throw new RuntimeException("输入路径不存在");
            }
            if (fs.exists(outputPath))  // 如果输出路径存在则删除
                fs.delete(outputPath, true);
            FileOutputFormat.setOutputPath(job, outputPath);
        } catch (IOException e) {
            logger.warn("获取FS对象异常!", e);
        } finally {
            try {
                fs.close();
            } catch (IOException e) {
                // do nothing
            }
        }
    }
}
