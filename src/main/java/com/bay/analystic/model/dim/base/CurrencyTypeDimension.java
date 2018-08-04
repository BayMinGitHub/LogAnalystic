package com.bay.analystic.model.dim.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Description: 货币类型维度类
 * Author by BayMin, Date on 2018/8/4.
 */
public class CurrencyTypeDimension extends BaseDimension {
    private int id;
    private String currencyName;

    public CurrencyTypeDimension() {
    }

    public CurrencyTypeDimension(String currencyName) {
        this.currencyName = currencyName;
    }

    public CurrencyTypeDimension(int id, String currencyName) {
        this.id = id;
        this.currencyName = currencyName;
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (this == o)
            return 0;
        CurrencyTypeDimension other = (CurrencyTypeDimension) o;
        int tmp = this.id - other.id;
        if (tmp != 0)
            return tmp;
        tmp = this.currencyName.compareTo(other.currencyName);
        return tmp;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.id);
        out.writeUTF(this.currencyName);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.currencyName = in.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CurrencyTypeDimension that = (CurrencyTypeDimension) o;

        if (id != that.id) return false;
        return currencyName.equals(that.currencyName);
    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + currencyName.hashCode();
        return result;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getCurrencyName() {
        return currencyName;
    }

    public void setCurrencyName(String currencyName) {
        this.currencyName = currencyName;
    }
}
