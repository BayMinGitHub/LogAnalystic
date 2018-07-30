package com.qianfeng.analystic.mr.out;

import com.qianfeng.analystic.model.dim.base.BaseDimension;
import com.qianfeng.analystic.model.dim.value.OutputValueBaseWritable;
import com.qianfeng.analystic.service.IDimensionConvert;
import com.qianfeng.common.KpiType;
import org.apache.hadoop.mapreduce.*;

import javax.security.auth.login.Configuration;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Map;


/**
 * @Description: 自定义输出到MySQL的输出格式类
 * Author by BayMin, Date on 2018/7/30.
 */
public class OutputWritterFormat extends OutputFormat<BaseDimension, OutputValueBaseWritable> {
    // DBOutFormat
    @Override
    public RecordWriter<BaseDimension, OutputValueBaseWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {

        return null;
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


        public OutputWritterRecordWritter() {
        }

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
                if (map.get(kpi) == null)
                    map.put(kpi, conn.prepareStatement(kpi.kpiName));
                else {
                    ps = map.get(kpi);
                    count = batch.get(kpi);
                    count++;
                }
                // 将批量的值更新到batch中
                this.batch.put(kpi, count);
            } catch (Exception e) {

            }
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {

        }
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
        // 不用检测输出空间
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        return null;


    }
}