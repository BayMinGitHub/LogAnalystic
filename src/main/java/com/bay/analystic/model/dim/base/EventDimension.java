package com.bay.analystic.model.dim.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Description: 事件维度类
 * Author by BayMin, Date on 2018/8/2.
 */
public class EventDimension extends BaseDimension {
    private int id;
    private String category;
    private String action;

    public EventDimension() {
    }

    public EventDimension(String category, String action) {
        this.category = category;
        this.action = action;
    }

    public EventDimension(int id, String category, String action) {
        this(category, action);
        this.id = id;
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (this == o)
            return 0;
        EventDimension other = (EventDimension) o;
        int tmp = this.id - other.id;
        if (tmp != 0)
            return tmp;
        tmp = this.category.compareTo(other.category);
        if (tmp != 0)
            return tmp;
        tmp = this.action.compareTo(other.category);
        return tmp;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.id);
        out.writeUTF(this.category);
        out.writeUTF(this.action);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.category = in.readUTF();
        this.action = in.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EventDimension that = (EventDimension) o;

        if (id != that.id) return false;
        if (!category.equals(that.category)) return false;
        return action.equals(that.action);
    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + category.hashCode();
        result = 31 * result + action.hashCode();
        return result;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }
}
