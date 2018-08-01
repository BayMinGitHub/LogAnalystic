package com.qianfeng.analystic.model.dim.out;

import com.qianfeng.analystic.model.dim.base.BaseDimension;
import com.qianfeng.analystic.model.dim.value.OutputValueBaseWritable;
import com.qianfeng.analystic.service.IDimensionConvert;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @Description: 操作最终结果表的接口
 * Author by BayMin, Date on 2018/7/30.
 */
public interface OutputWritter {
    /**
     * 将Reduce阶段的统计结果直接输出到MySQL库中
     *
     * @param conf    用于传递kpi
     * @param key     存储维度
     * @param value   存储统计值
     * @param ps      对应kpi的sql ps
     * @param convert 获取对应维度的id值
     * @throws IOException
     * @throws SQLException
     */
    void outputWrite(Configuration conf, BaseDimension key, OutputValueBaseWritable value, PreparedStatement ps, IDimensionConvert convert) throws IOException, SQLException;
}
