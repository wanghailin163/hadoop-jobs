package com.ibeifeng.hadoop.mapreduce.join;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class DatajoinWritable implements Writable{

	private String tag;
	private String data;
	public DatajoinWritable(){
		
	}
	public DatajoinWritable(String tag,String data){
		this.tag=tag;
		this.data=data;
	}
	public void set(String tag,String data){
		this.setTag(tag);
		this.setData(data);
	}
	public String getTag() {
		return tag;
	}
	public void setTag(String tag) {
		this.tag = tag;
	}
	public String getData() {
		return data;
	}
	public void setData(String data) {
		this.data = data;
	}
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.getTag());
		out.writeUTF(this.getData());
	}
	public void readFields(DataInput in) throws IOException {
		this.setTag(in.readUTF());
		this.setData(in.readUTF());
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((data == null) ? 0 : data.hashCode());
		result = prime * result + ((tag == null) ? 0 : tag.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DatajoinWritable other = (DatajoinWritable) obj;
		if (data == null) {
			if (other.data != null)
				return false;
		} else if (!data.equals(other.data))
			return false;
		if (tag == null) {
			if (other.tag != null)
				return false;
		} else if (!tag.equals(other.tag))
			return false;
		return true;
	}
	@Override
	public String toString() {
		return "DatajoinWritable [tag=" + tag + ", data=" + data + "]";
	}
	
}
