package com.bay.analystic.model.dim.base;

import com.bay.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Description: 地区维度类
 * Author by BayMin, Date on 2018/8/1.
 */
public class LocationDimension extends BaseDimension {
    private int id;
    private String country;
    private String province;
    private String city;

    public LocationDimension() {
    }

    public LocationDimension(String country, String province, String city) {
        this.country = country;
        this.province = province;
        this.city = city;
    }

    public LocationDimension(int id, String country, String province, String city) {
        this(country, province, city);
        this.id = id;
    }

    /**
     * 构建平台维度的集合对象
     */
    public static List<LocationDimension> buildList(String country, String province, String city) {
        if (StringUtils.isEmpty(country)) {
            country = GlobalConstants.DEFAULT_VALUE;
        }
        if (StringUtils.isEmpty(province)) {
            province = GlobalConstants.DEFAULT_VALUE;
        }
        if (StringUtils.isEmpty(city)) {
            city = GlobalConstants.DEFAULT_VALUE;
        }
        List<LocationDimension> li = new ArrayList<>();
        li.add(new LocationDimension(country, province, city));
        li.add(new LocationDimension(country, province, GlobalConstants.ALL_OF_VALUE));
        return li;
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (o == this)
            return 0;
        LocationDimension other = (LocationDimension) o;
        int tmp = this.id - other.id;
        if (tmp != 0)
            return tmp;
        tmp = this.country.compareTo(other.country);
        if (tmp != 0)
            return tmp;
        tmp = this.province.compareTo(other.province);
        if (tmp != 0)
            return tmp;
        tmp = this.city.compareTo(other.city);
        return tmp;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.id);
        out.writeUTF(this.country);
        out.writeUTF(this.province);
        out.writeUTF(this.city);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.country = in.readUTF();
        this.province = in.readUTF();
        this.city = in.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LocationDimension that = (LocationDimension) o;

        if (id != that.id) return false;
        if (!country.equals(that.country)) return false;
        if (!province.equals(that.province)) return false;
        return city.equals(that.city);
    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + country.hashCode();
        result = 31 * result + province.hashCode();
        result = 31 * result + city.hashCode();
        return result;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }
}
