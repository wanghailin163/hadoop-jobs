package com.myself.hadoop.mapreduce.mapjoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public class CustOrderMapOutKey implements WritableComparable<CustOrderMapOutKey>{
	

    private int custId;
    private int orderId;

    public void set(int custId, int orderId) {
        this.custId = custId;
        this.orderId = orderId;
    }
    
    public int getCustId() {
        return custId;
    }
    
    public int getOrderId() {
        return orderId;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(custId);
        out.writeInt(orderId);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        custId = in.readInt();
        orderId = in.readInt();
    }

    @Override
    public int compareTo(CustOrderMapOutKey o) {
        int res = Integer.compare(custId, o.custId);
        return res == 0 ? Integer.compare(orderId, o.orderId) : res;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof CustOrderMapOutKey) {
            CustOrderMapOutKey o = (CustOrderMapOutKey)obj;
            return custId == o.custId && orderId == o.orderId;
        } else {
            return false;
        }
    }
    
    @Override
    public String toString() {
        return custId + "\t" + orderId;
    }


}
