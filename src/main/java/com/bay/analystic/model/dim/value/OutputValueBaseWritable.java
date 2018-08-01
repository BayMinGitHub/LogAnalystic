package com.bay.analystic.model.dim.value;

import com.bay.common.KpiType;
import org.apache.hadoop.io.Writable;

/**
 * @Description: map或者是reduce阶段输出value类型的顶级父类
 * Author by BayMin, Date on 2018/7/27.
 */
public abstract class OutputValueBaseWritable implements Writable {
    /**
     * 获取kpi
     */
    public abstract KpiType getKpi(); // 获取一个kpi的抽象方法
}
