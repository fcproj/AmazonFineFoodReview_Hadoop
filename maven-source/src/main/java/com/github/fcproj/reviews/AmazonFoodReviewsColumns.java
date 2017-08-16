package com.github.fcproj.reviews;

/**
 * Constants for columns of the Amazon Fine Food Review CSV file
 * @author fabrizio
 *
 */
public class AmazonFoodReviewsColumns {
	
	public static final int ID = 0;
	public static final int PROD_ID = 1;//unique identifier for the product
	public static final int USER_ID = 2;//unique identifier for the user
	public static final int PROFILE = 3;
	public static final int HELP_NUM = 4;//number of users who found the review helpful
	public static final int HELP_DEN = 5;//number of users who graded the review
	public static final int SCORE = 6;//rating between 1 and 5
	public static final int TIME = 7;//timestamp of the review expressed in Unix time
	public static final int SUMMARY = 8;//summary of the review
	public static final int TEXT = 9;//text of the review
	
}
