package com.qianfeng.analystic.mr.nu;

import com.qianfeng.analystic.model.dim.base.BaseDimension;
import com.qianfeng.analystic.model.dim.value.OutputValueBaseWritable;
import com.qianfeng.analystic.mr.out.OutputWritter;
import com.qianfeng.analystic.service.IDimensionConvert;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @Description: 为新增用户的ps赋值
 * Author by BayMin, Date on 2018/7/30.
 */
public class NewUserOutputWritter implements OutputWritter {
    @Override
    public void outputWrite(Configuration conf, BaseDimension key, OutputValueBaseWritable value, PreparedStatement ps, IDimensionConvert convert) throws IOException, SQLException {

    }
}
