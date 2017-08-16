package com.github.fcproj.reviews.users;

import java.io.IOException;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

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
 * Given Amazon Fine Food Review CSV files: for each user, 10 preferred products (with highest score)
 * Output: userid, product ID, score sorted by userid.
 * 
 * A single pass is enough. In fact, a reducer gets all the score of a userId, and it computes the top 10 scores.
 * Two passes are needed if we need to compute top k of keys. In that case, each reducer would compute its own top k keys,
 * and a second pass must aggregates results using a single reducer.
 * 
 * The output is:
 * userid \t PRODID \t SCORE
 * 
 * The input is:
 * - a directory containing one or more input files
 * - an output directory
 * 
 * @author fabrizio
 *
 */
public class TopFavouriteProducts {

	private static final int TOP_K = 10;

	/** ***************************************************************************************
	 * Returns a userid with the object rating (=productID and the score)
	 * 
	 * @author fabrizio
	 *
	 * ***************************************************************************************/
	public static class Mapper1 extends Mapper<LongWritable,Text,Text,ReviewWritable> {

		//To store results
		//The reason for defining REVIEW and USERID in the class rather than
		//inside the method is purely one of efficiency. The map() method will be called as
		//many times as there are records (in a split, for each JVM). Reducing the number
		//of objects created inside the map() method can increase performance and reduce
		//garbage collection
		private static ReviewWritable REVIEW = new ReviewWritable();
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
					if(prodID!=null && userID!=null){
						REVIEW.set(new Text(prodID), new DoubleWritable(score));
						USERID.set(userID);
						ctx.write(USERID, REVIEW);
					}				
				}
				catch(Exception e){
					e.printStackTrace();
				}
			}	
		}
	}

	/** ***************************************************************************************
	 * Computes top K products for each user
	 * 
	 * @author fabrizio
	 *
	 * ***************************************************************************************/
	public static class Reducer1 extends Reducer<Text,ReviewWritable,Text,ReviewWritable> {

		//queue to sort products
		private Map<String, PriorityQueue<ReviewWritable>> user2queue;
		private Text USER_ID = new Text();

		@Override
		protected void setup(Context ctx) { 
			user2queue = new TreeMap<String, PriorityQueue<ReviewWritable>>();
		}

		//compute the ordered queue of TOP_K element for each userid
		@Override
		public void reduce(Text key, Iterable<ReviewWritable> values, 
				Context ctx) throws IOException, InterruptedException {	
			
			//get the userid queue
			PriorityQueue<ReviewWritable> queue = user2queue.get(key.toString());
			if(queue==null){
				queue = new PriorityQueue<ReviewWritable>(TOP_K);
				user2queue.put(key.toString(), queue);
			}
			//for each product
			for (ReviewWritable value : values) {
				//add the score in the queue: smaller on top, bigger on tail
				//we need to create a new ReviewWritable, or only the last object will be considered
				//in fact, the iterator in the Hadoop reducer uses a single object whose contents is changed each time it goes to the next value
				//in addition, tmp cannot be defined as class variable for the same reason, we keep a reference in a queue so we need a new object
				ReviewWritable tmp = new ReviewWritable(new Text(value.getProductID().toString()), new DoubleWritable(value.getScore().get()));
				queue.add(tmp);
				if (queue.size() > TOP_K) {
					//remove the head, so the smallest score
					queue.remove();
				}
			}	
		}

		//reverse sort the queue for each useris and send results to the output
		@Override
		protected void cleanup(Context ctx) 
				throws IOException, InterruptedException {	
			
			for(String userID: user2queue.keySet()){
				
				List<ReviewWritable> topKProducts = new ArrayList<ReviewWritable>();
				USER_ID.set(userID);
				
				//feed the list from the queue 
				PriorityQueue<ReviewWritable> queue = user2queue.get(userID);
				while (!queue.isEmpty()) {
					topKProducts.add(queue.remove());
				}
				
				//scan from last to first, to get descending order
				for (int i = topKProducts.size() - 1; i >= 0; i--) {
					ReviewWritable prod = topKProducts.get(i);		
					//we need to create a new ReviewWritable, or only the last object will be considered
					//in addition, tmp cannot be defined as class variable for the same reason, we keep a reference in a queue so we need a new object
					ReviewWritable tmp = new ReviewWritable(new Text(prod.getProductID().toString()), new DoubleWritable(prod.getScore().get()));					
					ctx.write(USER_ID, tmp);
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
			System.err.println("Usage: TopFavouriteProducts <directory-in> <directory-out>");
			System.exit(2);
		}
		
		Job job = Job.getInstance(conf);
		job.setJobName("TopFavouriteProducts");
		
		job.setJarByClass(TopFavouriteProducts.class);
		job.setMapperClass(Mapper1.class);
		job.setCombinerClass(Reducer1.class);//this is possible because reducers types are <x,y,x,y>
		job.setReducerClass(Reducer1.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ReviewWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(ReviewWritable.class);

		//a directory containing one or more input files, otherwise we would use addInputPath for a single file
		FileInputFormat.setInputPaths(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		int flag =  job.waitForCompletion(true) ? 0 : 1;
		long end=System.currentTimeMillis();
		System.out.println("#Execution time in seconds : "+ (end-start)/1000.0);

		System.exit(flag);
	}
}
