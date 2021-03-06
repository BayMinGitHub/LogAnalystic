package com.bay.analystic.mr.pv;

import com.bay.analystic.model.dim.key.StatsUserDimension;
import com.bay.analystic.model.dim.out.OutputWritterFormat;
import com.bay.analystic.model.dim.value.reduce.MapWritableValue;
import com.bay.analystic.model.dim.value.map.TimeOutputValue;
import com.bay.common.EventLogConstants;
import com.bay.common.GlobalConstants;
import com.bay.util.TimeUtil;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.util.List;

/**
 * @Description: 驱动类
 * Author by BayMin, Date on 2018/7/30.
 */
public class PageViewRunner implements Tool {
    private static final Logger logger = Logger.getLogger(PageViewRunner.class);
    private Configuration conf = new Configuration();

    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(), new PageViewRunner(), args);
        } catch (Exception e) {
            logger.warn("运行PV计算失败", e);
        }
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf.addResource("query-mapping.xml");
        this.conf.addResource("output-writter.xml");
        this.conf.addResource("total-mapping.xml");
        this.conf = HBaseConfiguration.create(this.conf);
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        this.setArgs(args, conf);
        // 获取作业
        Job job = Job.getInstance(conf, "page view");
        job.setJarByClass(PageViewRunner.class);
        // 初始化mapper类
        // 集群运行时还需要将Jar放入,以便于添加依赖Jar包
        // conf.set("mapred.jar", "target/LogAnalystic-1.0-SNAPSHOT.jar");
        // addDependencyJars:true是集群运行,false是本地提交本地运行
        // TableMapReduceUtil.initTableMapperJob(this.getScans(job), PageViewMapper.class, StatsUserDimension.class,
        //         TimeOutputValue.class, job, false);
        TableMapReduceUtil.initTableMapperJob(this.getScans(job), PageViewMapper.class, StatsUserDimension.class,
                TimeOutputValue.class, job);

        // reducer的设置
        job.setReducerClass(PageViewReducer.class);
        job.setOutputKeyClass(StatsUserDimension.class);
        job.setOutputValueClass(MapWritableValue.class);
        // 设置输出的格式类型
        job.setOutputFormatClass(OutputWritterFormat.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

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
     * 获取扫描的集合对象
     */
    private List<Scan> getScans(Job job) {
        Configuration conf = job.getConfiguration();
        // 获取运行日期
        long start = TimeUtil.parserString2Long(conf.get(GlobalConstants.RUNNING_DATE));
        long end = start + GlobalConstants.DAY_OF_MILISECONDS;
        // 获取scan对象
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(start + ""));
        scan.setStopRow(Bytes.toBytes(end + ""));
        // 定义过滤器
        FilterList fl = new FilterList();
        fl.addFilter(new SingleColumnValueFilter(Bytes.toBytes(EventLogConstants.HBASE_COLUMN_FAMILY),
                Bytes.toBytes(EventLogConstants.EVENT_COLUMN_NAME_EVENT_NAME),
                CompareFilter.CompareOp.EQUAL, Bytes.toBytes(EventLogConstants.EventEnum.PAGEVIEW.alias)));
        // TODO 注意上面的过滤条件!!!和自己的数据!!!
        // 设置扫描的字段
        String[] fields = {
                EventLogConstants.EVENT_COLUMN_NAME_SERVER_TIME,
                EventLogConstants.EVENT_COLUMN_NAME_PLATFORM,
                EventLogConstants.EVENT_COLUMN_NAME_EVENT_NAME,
                EventLogConstants.EVENT_COLUMN_NAME_BROWSER_NAME,
                EventLogConstants.EVENT_COLUMN_NAME_BROWSER_VERSION,
                EventLogConstants.EVENT_COLUMN_NAME_CURRENT_URL
        };
        // 将扫描的字段添加到filter中
        fl.addFilter(this.getFilters(fields));
        // 将scan设置表名
        scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes(EventLogConstants.HBASE_TABLE_NAME));
        // 将filter添加到scan中
        scan.setFilter(fl);
        return Lists.newArrayList(scan); // Google的一个api
    }

    /**
     * 设置扫描的列
     */
    private Filter getFilters(String[] fields) {
        int length = fields.length;
        byte[][] filters = new byte[length][];
        for (int i = 0; i < length; i++) {
            filters[i] = Bytes.toBytes(fields[i]);
        }
        return new MultipleColumnPrefixFilter(filters);
    }
}