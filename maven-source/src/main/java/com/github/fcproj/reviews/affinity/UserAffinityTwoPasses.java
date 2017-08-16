package com.github.fcproj.reviews.affinity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.github.fcproj.reviews.AmazonFoodReviewsColumns;
import com.github.fcproj.reviews.domain.CoupleWritable;

/**
 * Given Amazon Fine Food Review CSV files: computes users with same preferences. Thus, users who gave score >=4
 * to at least 3 common products.
 * 
 * Two passes:
 * - In the first pass, couple of users who scored >=4 the same product are generated. The output is a tmp file user1 \t user2 \t product
 * - In the second pass, couples are sorted so that user1<user2. Then, if the couple has at least 3 products, it goes to the output
 * 
 * The output is:
 * userid1 \t userid2 \t prod1 \t ... \t prodn
 * sorted by userid1 and without duplicates.
 * The output table has not a fixed number of column, but first two columns always represent the couple
 * 
 * The input is:
 * - a directory containing one or more input files
 * - an output directory
 * 
 * @author fabrizio
 *
 */
public class UserAffinityTwoPasses {

	private static final int MIN_SCORE = 4;
	private static final int MIN_PRODUCTS = 3;

	/** ***************************************************************************************
	 * Returns a productid with the userid of the user who gave score >= MIN_SCORE
	 * 
	 * @author fabrizio
	 *
	 * ***************************************************************************************/
	public static class Mapper1 extends Mapper<LongWritable,Text,Text,Text> {

		//To store results
		//The reason for defining PRODUCTID and USERID in the class rather than
		//inside the method is purely one of efficiency. The map() method will be called as
		//many times as there are records (in a split, for each JVM). Reducing the number
		//of objects created inside the map() method can increase performance and reduce
		//garbage collection
		private static Text PRODUCTID = new Text();
		private static Text USERID = new Text();

		@Override
		public void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
			String[] cols = (value.toString()).split("\t");
			//check data correctness
			if(cols!=null && cols.length==10){
				try {
					String userID = cols[AmazonFoodReviewsColumns.USER_ID];
					String prodID = cols[AmazonFoodReviewsColumns.PROD_ID];
					int score = Integer.parseInt(cols[AmazonFoodReviewsColumns.SCORE]);
					if(prodID!=null && userID!=null && score>=MIN_SCORE){
						PRODUCTID.set(prodID);
						USERID.set(userID);
						ctx.write(PRODUCTID, USERID);
					}				
				}
				catch(Exception e){
					e.printStackTrace();
				}
			}	
		}
	}

	/** ***************************************************************************************
	 * Computes Couples related to each product
	 * Putput: Couple -> ProductID
	 * 
	 * @author fabrizio
	 *
	 * ***************************************************************************************/
	public static class Reducer1 extends Reducer<Text,Text,CoupleWritable,Text> {
		private Text PRODUCT = new Text();

		//compute the ordered queue of TOP_K element for each userid
		@Override
		public void reduce(Text key, Iterable<Text> values, 
				Context ctx) throws IOException, InterruptedException {	
			String productID = key.toString();
			//generate couples
			List<Text> tmpUsers = new ArrayList<Text>();
			//in fact, the iterator in the Hadoop reducer uses a single object whose contents is changed each time it goes to the next value
			//in addition, tmp cannot be defined as class variable for the same reason, we keep a reference in a queue so we need a new object
			for(Text userID: values)
				tmpUsers.add(new Text(userID.toString()));
			for(int i=0; i<tmpUsers.size();i++)
				for(int j=i+1; j<tmpUsers.size();j++){
					Text t1 = tmpUsers.get(i);
					Text t2 = tmpUsers.get(j);
					if(t1.toString()!=t2.toString()){
						CoupleWritable couple = new CoupleWritable(t1, t2);
						PRODUCT.set(productID);
						ctx.write(couple, PRODUCT);
					}
				}
		}
	}

	/** ***************************************************************************************
	 * Returns a couple (sorted by user to help the partitioner) with the productID
	 * 
	 * @author fabrizio
	 *
	 * ***************************************************************************************/
	public static class Mapper2 extends Mapper<LongWritable,Text,CoupleWritable,Text> {

		@Override
		protected void map(LongWritable key, Text value, Context ctx)
				throws IOException, InterruptedException {
			String[] cols = (value.toString()).split("\t");
			CoupleWritable couple = null;
			String user1= cols[0];
			String user2 = cols[1];
			if(user1.compareTo(user2)<0)
				couple = new CoupleWritable(new Text(user1), new Text(user2));
			else
				couple = new CoupleWritable(new Text(user2), new Text(user1));
			ctx.write(couple, new Text(cols[2]));
		}
	}
	
	/** ***************************************************************************************
	 * Last Step: if the list of products is grater or equals to MIN_PRODUCTS, write the couple
	 * 
	 * @author fabrizio
	 *
	 * ***************************************************************************************/
	public static class Reducer2 extends Reducer<CoupleWritable,Text,CoupleWritable,Text> {
		private Text LIST_PRODUCTS = new Text();

		//compute the ordered queue of TOP_K element for each userid
		@Override
		public void reduce(CoupleWritable key, Iterable<Text> values, 
				Context ctx) throws IOException, InterruptedException {	
			
			//generate couples
			Set<Text> tmpProducts = new HashSet<Text>();
			//in fact, the iterator in the Hadoop reducer uses a single object whose contents is changed each time it goes to the next value
			//in addition, tmp cannot be defined as class variable for the same reason, we keep a reference in a queue so we need a new object
			for(Text prodID: values)
				tmpProducts.add(new Text(prodID.toString()));
			if(tmpProducts.size()>=MIN_PRODUCTS){
				String productsTabDel = "";
				for(Text prodID: tmpProducts)
					productsTabDel +=prodID+"\t";
				if(productsTabDel.endsWith("\t"))
					productsTabDel.subSequence(0, productsTabDel.length()-2);
				LIST_PRODUCTS.set(productsTabDel);
				ctx.write(new CoupleWritable(new Text(key.getA().toString()), new Text(key.getB().toString())), LIST_PRODUCTS);
			}
		}
	}
	
	/**
	 * Do the job
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		long start=System.currentTimeMillis();

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: UserAffinityTwoPassesv <directory-in> <directory-out>");
			System.exit(2);
		}
		
		Path input = new Path(args[0]);
		Path temp1 = new Path("temp");
		Path output = new Path(args[1]);
		
		Job job1 = Job.getInstance(conf);
		job1.setJobName("UserAffinityTwoPasses-pass-1");
		
		job1.setJarByClass(UserAffinityTwoPasses.class);
		job1.setMapperClass(Mapper1.class);
		job1.setReducerClass(Reducer1.class);	
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(CoupleWritable.class);
		job1.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job1, input);
		FileOutputFormat.setOutputPath(job1, temp1);
		int flag = job1.waitForCompletion(true) ? 0 : 1;
		if (flag!=0) {
			System.out.println("Job1 failed, exiting");
			System.exit(flag);
		}

		Job job2 = Job.getInstance(conf, "UserAffinityTwoPasses-pass-2");
		FileInputFormat.setInputPaths(job2, temp1);
		FileOutputFormat.setOutputPath(job2, output);
		job2.setJarByClass(UserAffinityTwoPasses.class);
		job2.setMapperClass(Mapper2.class);
		job2.setReducerClass(Reducer2.class);
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setMapOutputKeyClass(CoupleWritable.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(CoupleWritable.class);
		job2.setOutputValueClass(Text.class);
		
		flag =  job2.waitForCompletion(true) ? 0 : 1;
		long end=System.currentTimeMillis();
		System.out.println("#Execution time in seconds : "+ (end-start)/1000.0);

		System.exit(flag);
	}

}
