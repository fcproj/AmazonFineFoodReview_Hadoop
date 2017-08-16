package com.github.fcproj.reviews.domain;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * This object represents a score for a product
 * Comparator: by score (ascedning), then by productid
 * @author fabrizio
 *
 */
public class ReviewWritable implements WritableComparable<ReviewWritable> {
	
    private Text productID;
    private DoubleWritable score;

    public ReviewWritable() {
    }

    public ReviewWritable(Text productID, DoubleWritable score) {
        this.productID = productID;
        this.score = score;
    }

    public void readFields(DataInput in) throws IOException {
    	productID = new Text(in.readUTF());
    	score = new DoubleWritable(Double.parseDouble(in.readUTF()));
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(productID.toString());
        out.writeUTF(score.toString());
    }

    public void set(Text productID, DoubleWritable score) {
    	this.productID = productID;
    	this.score = score;
    }

    @Override
    public String toString() {
        return productID.toString() + "\t" + score.toString();
    }

    @Override
    public int hashCode() {
        return productID.hashCode() + score.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof ReviewWritable) {
        	ReviewWritable rev = (ReviewWritable) o;
            return productID.equals(rev.productID)
                    && score.equals(rev.score);
        }
        return false;
    }

    /**
     * First, compare by score
     */
    public int compareTo(ReviewWritable tp) {
        int cmp = score.compareTo(tp.score);
        if (cmp != 0) {
            return cmp;
        }
        return productID.compareTo(tp.productID);
    }

	public Text getProductID() {
		return productID;
	}

	public DoubleWritable getScore() {
		return score;
	}
    
    

}
