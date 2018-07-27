package com.qianfeng.analystic.model.dim.base;

import com.qianfeng.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Description: 平台维度类
 * Author by BayMin, Date on 2018/7/27.
 */
public class PlatFormDimension extends BaseDimension {
    private int id;
    private String platformName;

    public PlatFormDimension() {
    }

    public PlatFormDimension(String platformName) {
        this.platformName = platformName;
    }

    public PlatFormDimension(int id, String platformName) {
        this(platformName);
        this.id = id;
    }

    /**
     * 构建平台维度的集合对象
     */
    public static List<PlatFormDimension> buildList(String platformName) {
        if (StringUtils.isEmpty(platformName)) {
            platformName = GlobalConstants.DEFAULT_VALUE;
        }
        List<PlatFormDimension> li = new ArrayList<PlatFormDimension>();
        li.add(new PlatFormDimension(platformName));
        li.add(new PlatFormDimension(GlobalConstants.ALL_OF_VALUE));
        return li;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.id);
        out.writeUTF(this.platformName);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.platformName = in.readUTF();
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (this == o) {
            return 0;
        }
        PlatFormDimension other = (PlatFormDimension) o;
        int tmp = this.id - other.id;
        if (tmp != 0) {
            return tmp;
        }
        tmp = this.platformName.compareTo(other.platformName);
        return tmp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PlatFormDimension that = (PlatFormDimension) o;

        if (id != that.id) return false;
        return platformName != null ? platformName.equals(that.platformName) : that.platformName == null;
    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + (platformName != null ? platformName.hashCode() : 0);
        return result;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getPlatformName() {
        return platformName;
    }

    public void setPlatformName(String platformName) {
        this.platformName = platformName;
    }
}

