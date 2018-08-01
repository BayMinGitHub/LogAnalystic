package com.qianfeng.analystic.mr.nu;

import com.google.common.collect.Lists;
import com.qianfeng.analystic.model.dim.base.DateDimension;
import com.qianfeng.analystic.model.dim.key.StatsUserDimension;
import com.qianfeng.analystic.model.dim.value.TimeOutputValue;
import com.qianfeng.analystic.model.dim.value.MapWritableValue;
import com.qianfeng.analystic.model.dim.out.OutputWritterFormat;
import com.qianfeng.analystic.service.IDimensionConvert;
import com.qianfeng.analystic.service.impl.IDimensionConvertImpl;
import com.qianfeng.common.DateEnum;
import com.qianfeng.common.EventLogConstants;
import com.qianfeng.common.GlobalConstants;
import com.qianfeng.util.JDBCUtil;
import com.qianfeng.util.TimeUtil;
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

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Description: 驱动类
 * Author by BayMin, Date on 2018/7/30.
 */
public class NewUserRunner implements Tool {
    private static final Logger logger = Logger.getLogger(NewUserRunner.class);
    private Configuration conf = new Configuration();

    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(), new NewUserRunner(), args);
        } catch (Exception e) {
            logger.warn("运行新增用户指标失败", e);
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
        Job job = Job.getInstance(conf, "new users");
        job.setJarByClass(NewUserRunner.class);
        // 初始化mapper类
        // 集群运行时还需要将Jar放入,以便于添加依赖Jar包
        // conf.set("mapred.jar", "target/LogAnalystic-1.0-SNAPSHOT.jar");
        // addDependencyJars:true是本地提交集群运行,false是本地提交本地运行
        TableMapReduceUtil.initTableMapperJob(this.getScans(job), NewUserMapper.class, StatsUserDimension.class,
                TimeOutputValue.class, job, false);

        // reducer的设置
        job.setReducerClass(NewUserReducer.class);
        job.setOutputKeyClass(StatsUserDimension.class);
        job.setOutputValueClass(MapWritableValue.class);
        // 设置输出的格式类型
        job.setOutputFormatClass(OutputWritterFormat.class);
        // return job.waitForCompletion(true) ? 0 : 1;

        // 计算新增总用户
        if (job.waitForCompletion(true)) {
            this.computeNewTotalUser(job);
            return 0;
        } else {
            return 1;
        }
    }

    /**
     * 计算新增的总用户
     * 1.获取运行当天的日期,然后再获取到运行当前前一天的日期,然后获取对应时间维度ID
     * 2.当对应时间维度ID都大于0,则正常计算:查询前一天的新增总用户,获取当天的新增用户
     */
    private void computeNewTotalUser(Job job) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        // 获取运行的日期
        try {
            String date = job.getConfiguration().get(GlobalConstants.RUNNING_DATE);
            long nowDay = TimeUtil.parserString2Long(date);
            long yesterday = nowDay - GlobalConstants.DAY_OF_MILISECONDS;
            // 获取对应的时间维度
            DateDimension nowDateDimension = DateDimension.buildDate(nowDay, DateEnum.DAY);
            DateDimension yesterdayDateDimension = DateDimension.buildDate(yesterday, DateEnum.DAY);

            int nowDimensionId = -1;
            int yesterdayDimensionId = -1;

            // 获取维度的Id
            IDimensionConvert convert = new IDimensionConvertImpl();
            nowDimensionId = convert.getDimensionIDByDimension(nowDateDimension);
            yesterdayDimensionId = convert.getDimensionIDByDimension(yesterdayDateDimension);

            // 判断昨天和今天的维度ID是否大于0
            conn = JDBCUtil.getConn();
            Map<String, Integer> map = new HashMap<>();
            if (yesterdayDimensionId > 0) {
                ps = conn.prepareStatement(conf.get(GlobalConstants.PREFIX_TOTAL + "new_total_user"));
                // 赋值
                ps.setInt(1, yesterdayDimensionId);
                // 执行
                rs = ps.executeQuery();
                while (rs.next()) {
                    int platformId = rs.getInt("platform_dimension_id");
                    int totalNewUser = rs.getInt("total_install_users");
                    // 存储
                    map.put(platformId + "", totalNewUser);
                }
            }
            if (nowDimensionId > 0) {
                ps = conn.prepareStatement(conf.get(GlobalConstants.PREFIX_TOTAL + "user_new_user"));
                // 赋值
                ps.setInt(1, nowDimensionId);
                // 执行
                rs = ps.executeQuery();
                while (rs.next()) {
                    int platformId = rs.getInt("platform_dimension_id");
                    int newUser = rs.getInt("new_install_users");
                    // 存储
                    if (map.containsKey(platformId + "")) {
                        newUser += map.get(platformId + "");
                    }
                    map.put(platformId + "", newUser);
                }
                // 更新新增总用户
                ps = conn.prepareStatement(conf.get(GlobalConstants.PREFIX_TOTAL + "user_new_update_user"));
                // 赋值
                for (Map.Entry<String, Integer> en : map.entrySet()) {
                    ps.setInt(1, nowDimensionId);
                    ps.setInt(2, Integer.parseInt(en.getKey()));
                    ps.setInt(3, en.getValue());
                    ps.setString(4, conf.get(GlobalConstants.RUNNING_DATE));
                    ps.setInt(5, en.getValue());
                    ps.execute();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            JDBCUtil.close(conn, ps, rs);
        }
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
                EventLogConstants.EVENT_COLUMN_NAME_UUID,
                EventLogConstants.EVENT_COLUMN_NAME_PLATFORM,
                EventLogConstants.EVENT_COLUMN_NAME_EVENT_NAME,
                EventLogConstants.EVENT_COLUMN_NAME_BROWSER_NAME,
                EventLogConstants.EVENT_COLUMN_NAME_BROWSER_VERSION
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