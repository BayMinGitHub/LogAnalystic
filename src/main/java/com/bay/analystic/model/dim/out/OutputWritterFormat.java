package com.bay.analystic.model.dim.out;

import com.bay.analystic.model.dim.base.BaseDimension;
import com.bay.analystic.model.dim.value.OutputValueBaseWritable;
import com.bay.analystic.service.IDimensionConvert;
import com.bay.analystic.service.impl.IDimensionConvertImpl;
import com.bay.common.GlobalConstants;
import com.bay.common.KpiType;
import com.bay.util.JDBCUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;


/**
 * @Description: 自定义输出到MySQL的输出格式类
 * Author by BayMin, Date on 2018/7/30.
 */
public class OutputWritterFormat extends OutputFormat<BaseDimension, OutputValueBaseWritable> {
    private static final Logger logger = Logger.getLogger(OutputWritterFormat.class);

    // DBOutFormat
    @Override
    public RecordWriter<BaseDimension, OutputValueBaseWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        Connection conn = JDBCUtil.getConn();
        IDimensionConvert convert = new IDimensionConvertImpl();
        return new OutputWritterRecordWritter(conn, conf, convert);
    }

    /**
     * 自定义封装writter的类
     * 需要继承RecordWriter
     */
    public static class OutputWritterRecordWritter extends RecordWriter<BaseDimension, OutputValueBaseWritable> {
        private Connection conn = null;
        private Configuration conf = null;
        private IDimensionConvert convert = null;
        // 用来判断Kpi的数据量
        private Map<KpiType, Integer> batch = new HashMap<>();
        // 用来存储Kpi存储的ps,方便下一次直接获取
        private Map<KpiType, PreparedStatement> map = new HashMap<>();

        public OutputWritterRecordWritter(Connection conn, Configuration conf, IDimensionConvert convert) {
            this.conn = conn;
            this.conf = conf;
            this.convert = convert;
        }

        @Override
        public void write(BaseDimension key, OutputValueBaseWritable value) throws IOException, InterruptedException {
            if (key == null || value == null)
                return;
            // k v不为空时
            // 获取kpi,然后根据kpi获取对应的sql语句
            KpiType kpi = value.getKpi();
            PreparedStatement ps = null;
            try {
                int count = 1; // 批量的起始值
                if (map.get(kpi) == null) {
                    ps = conn.prepareStatement(conf.get(kpi.kpiName));
                    // TODO 不要自己瞎胡写,最后出现错误都不知道在哪里!!!
                    map.put(kpi, ps);
                } else {
                    ps = map.get(kpi);
                    count = batch.get(kpi);
                    count++;
                }
                // 将批量的值更新到batch中
                this.batch.put(kpi, count);
                // 为ps赋值
                String outputWritterName = conf.get(GlobalConstants.PREFIX_OUTPUT + kpi.kpiName);
                Class classz = Class.forName(outputWritterName);
                OutputWritter outputWritter = (OutputWritter) classz.newInstance();
                outputWritter.outputWrite(conf, key, value, ps, convert); // 调用接口
                // 判断有多少个ps
                if (count % GlobalConstants.NUM_OF_BATCH == 0) {
                    ps.addBatch();
                    conn.commit();
                    // 执行完成,移除
                    batch.remove(kpi);
                }
            } catch (Exception e) {
                logger.warn("执行存储结果到MySQL异常", e);
            }
        }

        /**
         * 关闭之前,确保剩余的ps被执行一遍
         */
        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            try {
                for (Map.Entry<KpiType, PreparedStatement> en : map.entrySet()) {
                    en.getValue().executeBatch();
                }
            } catch (SQLException e) {
                logger.warn("关闭对象时,执行SQL异常", e);
            } finally {
                try {
                    for (Map.Entry<KpiType, PreparedStatement> en : map.entrySet()) {
                        en.getValue().close();
                        // map.remove(en.getKey());
                    }
                } catch (SQLException e) {
                    logger.warn("关闭ps时异常", e);
                } finally {
                    JDBCUtil.close(conn, null, null); // 关闭conn
                }
            }
        }
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
        // 不用检测输出空间
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new FileOutputCommitter(null, context);
    }
}