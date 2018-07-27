package com.qianfeng.analystic.mr.nu;

import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;

/**
 * @Description: 新增的用户和新增的总用户统计的Mapper类
 * Author by BayMin, Date on 2018/7/27.
 */
public class NewUserMapper extends TableMapper<Text, Text> {
}
