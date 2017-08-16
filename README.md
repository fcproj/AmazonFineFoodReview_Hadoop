# Amazon Fine Food Review Data Analysis

This project contains a set of Map-Reduce jobs that analyze **Amazon Food Review** data. This is a JAVA project
designed to be executed on Hadoop, both on single node cluster and on multi node environment.

## Input Dataset

The project refers to the [Amazon Food Reviews](http://snap.stanford.edu/data/web-FineFoods.html) dataset. 
The dataset is a CSV file that consists of 568,454 food reviews Amazon users from 1999 to 2012 (256,059 users and 74,258 products).
The columns in the table are:

* Id
* ProductId (unique identifier for the product)
* UserId (unqiue identifier for the user)
* ProfileName
* HelpfulnessNumerator (number of users who found the review helpful)
* HelpfulnessDenominator (number of users who graded the review)
* Score (rating between 1 and 5)
* Time (timestamp for the review expressed in Unix time)
* Summary (summary of the review)
* Text (text of the review)

Source (citation):

    J. McAuley and J. Leskovec. From amateurs to connoisseurs: modeling the evolution of user expertise through online reviews. WWW, 2013.


## Data Analysis Problems

Three problems were proposed in the context of the [Big Data course](http://torlone.dia.uniroma3.it/bigdata/) at the University
of Rome "Roma Tre". In particular, using Map-Reduce, write:

1. A job that computes, for each month, top-5 products with the highest average score. The output must include the ProduceId and the average score, sorted by Time. 
2. A job that computed, for each user, top-10 preferred products (i.e. products they rated with the highest score). The output must include the ProductId and the Score, sorted by UserId.
3. A job that can generate couples of users with similar preferences. Thus, users who gave at least score=4 to at least 3 common products. The output must include couples and the list of common products. The outut must be sorted by the first userid in the couple and must not contain duplicates.

##### Solution to Problem 1

The class `com.github.fcproj.reviews.users.TopHighestScore` implements the solution. A single pass is enough. In fact, each reducer gets all the products of a month, and it computes the average score per product and the top 5 products. Two passes are needed if we need to compute top k of keys and there is more than one reducer (a reducer can compute the top k keys of its keys). 

* Mapper: returns a month with the ReviewWritable object (i.e. productID and rating)
* Reducer: computes top K products for each month. Even if there are more reducers, each reducer receives all ReviewWritable of a given month (there are no two reducer receiving data of the same month, so a single pass is enough)

##### Solution to Problem 2

The class `com.github.fcproj.reviews.users.TopFavouriteProducts` implements the solution. In particular:

* Mapper: maps each UserId to a ReviewWritable object (that incaplulates the ProductId and the score)
* Reducer: in the **reduce()** method, it computes the ordered queue of TOP_K element for each userid, storing results in a map. Consider that we need to create a new temporary ReviewWritable while iterating over reducer input, otherwise only the last object will be considered. In fact, the iterator in the Hadoop reducer uses a single object whose content is changed each time it goes to the next value. In addition, the temporary ReviewWritable cannot be defined as class variable for the same reason; we keep a reference in a queue so we need a new object. In the **cleanup()** method, each queue is reversed to obtain a descending order.

Consider that a combiner can be defined using the same class of the reducer (in fact input/output keys are the same, and so are input/output values). 

A single pass is enough. In fact, a reducer gets all the score of a userId, and it computes the top 10 scores. Two passes are needed if we need to compute top k of keys. In that case, each reducer would compute its own top k keys, and a second pass must aggregates results using a single reducer.
	
##### Solution to Problem 3

The class `com.github.fcproj.reviews.affinity.UserAffinityTwoPasses` implements the solution. This job requires two passes. In the first pass, we create couple of users who rated a common product with a score greater or equals to 4. In the second pass, we aggregate all products of the same couple (couple <a,b>= couple <b,a>) and we output the couple only if there are at least 3 common products.
 
* Mapper1: maps a productid with the userid of the user who gave score >= MIN_SCORE
* Reducer1: for each product, generates a couple of users (only distinct users in the couple)
* Mapper2: generate a couple sorting user1<user2. Returns the couple (key) and the productid (value)
* Reducer2: if the list of productid is greater than MIN_PRODUCTS, writes the couple

Executon Time:
* 1999_2006.csv (5 MB) - 1 node, 2 cores and 48 GB - 44.3 s 
* 1999_2012.csv (~300 MB) - 1 node, 2 cores and 48 GB - 305.8 s
* 1999_2012.csv (~300 MB) - 2 nodes, each one has 2 cores and 48 GB - 302.289 s
* 1999_2012.csv (~300 MB) - 2 nodes, each one has 2 cores and 96 GB - 303.422 s







