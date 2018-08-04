package com.bay.analystic.model.dim.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Description: 支付方式维度类
 * Author by BayMin, Date on 2018/8/4.
 */
public class PaymentTypeDimension extends BaseDimension {
    private int id;
    private String paymentType;

    public PaymentTypeDimension() {
    }

    public PaymentTypeDimension(String paymentType) {
        this.paymentType = paymentType;
    }

    public PaymentTypeDimension(int id, String paymentType) {
        this.id = id;
        this.paymentType = paymentType;
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (this == o)
            return 0;
        PaymentTypeDimension other = (PaymentTypeDimension) o;
        int tmp = this.id - other.id;
        if (tmp != 0)
            return tmp;
        tmp = this.paymentType.compareTo(other.paymentType);
        return tmp;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.id);
        out.writeUTF(this.paymentType);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.paymentType = in.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PaymentTypeDimension that = (PaymentTypeDimension) o;

        if (id != that.id) return false;
        return paymentType.equals(that.paymentType);
    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + paymentType.hashCode();
        return result;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getPaymentType() {
        return paymentType;
    }

    public void setPaymentType(String paymentType) {
        this.paymentType = paymentType;
    }
}
