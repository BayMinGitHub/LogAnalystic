package com.qianfeng.analystic.mr.out;

import com.qianfeng.analystic.model.dim.base.BaseDimension;
import com.qianfeng.analystic.model.dim.value.OutputValueBaseWritable;
import com.qianfeng.analystic.service.IDimensionConvert;

import javax.security.auth.login.Configuration;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @Description: 将Reduce阶段的统计结果直接输出到MySQL库中
 * Author by BayMin, Date on 2018/7/30.
 */
public interface OutputWritter {
    /**
     * 操作最终结果表的接口
     */
    void outputWrite(Configuration conf, BaseDimension key, OutputValueBaseWritable value, PreparedStatement ps, IDimensionConvert convert) throws IOException, SQLException;
}
