package com.github.fcproj.reviews.domain;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * This object represents an unordered couple <a,b>
 * Comparator: by a (ascending), then by b
 * Equals: <a,b>=<b,a>
 * @author fabrizio
 *
 */
public class CoupleWritable implements WritableComparable<CoupleWritable>{

	private Text a;
    private Text b;

    public CoupleWritable() {
    }

    public CoupleWritable(Text a, Text b) {
        this.a = a;
        this.b = b;
    }

    public void readFields(DataInput in) throws IOException {
    	a = new Text(in.readUTF());
    	b = new Text(in.readUTF());
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(a.toString());
        out.writeUTF(b.toString());
    }

    public void set(Text a, Text b) {
    	 this.a = a;
         this.b = b;
    }

    @Override
    public String toString() {
        return a.toString() + "\t" + b.toString();
    }

    @Override
    public int hashCode() {
        return a.hashCode() + b.hashCode();
    }

    //<a,b>=<b,a>
    @Override
    public boolean equals(Object o) {
        if (o instanceof CoupleWritable) {
        	CoupleWritable couple = (CoupleWritable) o;
            return (this.a.equals(couple.a) && this.b.equals(couple.b)) || 
            	   (this.a.equals(couple.b) && this.b.equals(couple.a));
        }
        return false;
    }

    /**
     * First, compare by a
     */
    public int compareTo(CoupleWritable couple) {
        int cmp = this.a.compareTo(couple.a);
        if (cmp != 0) {
            return cmp;
        }
        return this.b.compareTo(couple.b);
    }

	public Text getA() {
		return a;
	}

	public Text getB() {
		return b;
	}
    
}
