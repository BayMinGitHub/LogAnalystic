package com.bay.analystic.service;

import com.bay.analystic.model.dim.base.BaseDimension;
import com.bay.analystic.model.dim.value.OutputValueBaseWritable;

import java.io.IOException;
import java.sql.SQLException;

/**
 * @Description: 根据维度对象获取对应维度的id接口
 * Author by BayMin, Date on 2018/7/27.
 */
public interface IDimensionConvert {
    // 据维度对象获取对应维度的id接口
    int getDimensionIDByDimension(BaseDimension baseDimension) throws IOException, SQLException;

    int getDimensionIDByDimension(OutputValueBaseWritable outputValueBaseWritable) throws IOException, SQLException;
}
