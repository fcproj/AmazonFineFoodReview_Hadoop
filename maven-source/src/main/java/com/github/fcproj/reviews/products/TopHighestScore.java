package com.github.fcproj.reviews.products;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
import com.github.fcproj.reviews.domain.ReviewWritable;

/**
 * Given Amazon Fine Food Review CSV files: for each month, 5 products with highest average score.
 * Output: product ID, average score, sorted by timestamp 
 * 
 * A single pass is enough. In fact, a reducer gets all the products of a month, and it computes the average score per product and the top 5 products.
 * Two passes are needed if we need to compute top k of keys. 
 * 
 * The output is:
 * MONTH\tPRODID\tAVERAGESCORE
 * 
 * The input is:
 * - a directory containing one or more input files
 * - an output directory
 * 
 * @author fabrizio
 *
 */
public class TopHighestScore {

	private static final int TOP_K = 5;
	
	/**
	 * Returns a month with the ReviewWritable object (= productID and the rating)
	 * @author fabrizio
	 *
	 */
	public static class Mapper1 extends Mapper<LongWritable,Text,Text,ReviewWritable> {
		
		private static ReviewWritable REVIEW = new ReviewWritable();
		@Override
		public void map(LongWritable key, Text value, 
				Context ctx) throws IOException, InterruptedException {
			String[] cols = (value.toString()).split("\t");
			//check data correctness
			if(cols!=null && cols.length==10){
				try {
					String prodID = cols[AmazonFoodReviewsColumns.PROD_ID];
					int score = Integer.parseInt(cols[AmazonFoodReviewsColumns.SCORE]);
					long date = Long.parseLong(cols[AmazonFoodReviewsColumns.TIME]);
					String month = null;
					if(prodID!=null){
						Date time=new Date(date*1000);
						SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM");
						month = sdf.format(time);
						if(month!=null){
							REVIEW.set(new Text(prodID), new DoubleWritable(score));
							ctx.write(new Text(month), REVIEW);
						}
					}				
				}
				catch(Exception e){
					e.printStackTrace();
				}
			}	
		}
	}

	/**
	 * Computes top K products for each month
	 * @author fabrizio
	 *
	 */
	public static class Reducer1 extends Reducer<Text,ReviewWritable,Text,ReviewWritable> {

		private Map<String, ProductMean> TimeProduct2Mean = new HashMap<String, ProductMean>();
		private static ReviewWritable REVIEW = new ReviewWritable();

		protected class ProductMean implements Comparable<ProductMean>{
			String productID;
			Double sum;//incremental sum of scores for this product
			Integer counter;//number of ratings
			Double mean;//mean, to be computed when all products have been parsed
			ProductMean(String productID, Double sum){
				this.productID = productID;
				this.sum = sum;
				this.counter = 1;
			}
			void increase(Double value){
				this.sum = this.sum + value;
				this.counter = this.counter + 1;
			}
			void computeMean(){
				this.mean = this.sum/this.counter;
			}
			public int compareTo(ProductMean obj) {
				if (this.mean < obj.mean) return 1;
				if (this.mean > obj.mean) return -1;
				return this.productID.compareTo(obj.productID);
			}
			public String toString(){
				return productID+" mean:"+mean;
			}
		}

		/*
		 * Prepare the map that will be parsed in cleanup.
		 * The map is TIME_ProdID -> ProductMean
		 * ProductMean contains the product ID and the mean (which must be computed before sorting elements)
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		@Override
		public void reduce(Text key, Iterable<ReviewWritable> values, 
				Context ctx) throws IOException, InterruptedException {

			for (ReviewWritable value : values) {			
				//combine month to product ID
				String productID = value.getProductID().toString();
				String timeProduct = key.toString()+"_"+productID;
				if(TimeProduct2Mean.containsKey(timeProduct)){
					TimeProduct2Mean.get(timeProduct).increase((double)value.getScore().get());
				}
				else{
					TimeProduct2Mean.put(timeProduct, new ProductMean(productID, (double)value.getScore().get()));
				}
			}			
		}

		@Override
		protected void cleanup(Context ctx) throws IOException, InterruptedException {

			//map time 2 sorted products
			Map<String, Set<ProductMean>> result = new TreeMap<String, Set<ProductMean>>();
			for(String time2productID: this.TimeProduct2Mean.keySet()){
				String[] parts = time2productID.split("_");
				String time = parts[0];	
				
				//it's time to compute the mean
				ProductMean element = this.TimeProduct2Mean.get(time2productID);
				element.computeMean();

				//inserting elements in descending order, defined by compareTo of ProductMean
				if(result.get(time)==null){
					Set<ProductMean> sortedResults = new TreeSet<ProductMean>();
					sortedResults.add(element);
					result.put(time, sortedResults);
				}
				else
					result.get(time).add(element);	
			}

			//getting the TOP_K products per month
			for(String time: result.keySet()){
				int counter = 1;
				for(ProductMean mean: result.get(time)){
					if(counter>TOP_K)
						break;
					REVIEW.set(new Text(mean.productID), new DoubleWritable(mean.mean));
					ctx.write(new Text(time), REVIEW);
					counter++;
				}
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
			System.err.println("Usage: TopHighestScore <directory-in> <directory-out>");
			System.exit(2);
		}

		Job job = Job.getInstance(conf);
		job.setJobName("TopHighestScore-pass-1");
		job.setJarByClass(TopHighestScore.class);
		job.setMapperClass(Mapper1.class);
		job.setReducerClass(Reducer1.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ReviewWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(ReviewWritable.class);
		
		//a directory containing one or more input files, otherwise we would use addInputPath for a single file
		FileInputFormat.setInputPaths(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		int flag = job.waitForCompletion(true) ? 0 : 1;
		long end=System.currentTimeMillis();
		System.out.println("#Execution time in seconds : "+ (end-start)/1000.0);

		System.exit(flag);
	}

}