import org.apache.spark
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.{SparkConf, SparkContext}
// $example on$
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
// $example off$

object Recomendation {
  def main(args: Array[String]): Unit = {

    //spark context
    val conf:SparkConf = new SparkConf().setAppName("Video Recomendation system").setMaster("local")
    val sc:SparkContext = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    //rating file
    val ratigsFile = "data/ratings.csv"
    val df1 = sqlContext.read.format("com.databricks.spark.csv").option("header", true).load(ratigsFile)
    val ratingsDF = df1.select(df1.col("userId"), df1.col("movieId"), df1.col("rating"), df1.col("timestamp"))
    ratingsDF.show(false)

    //movie file
    val moviesFile = "data/movies.csv"
    val df2 = sqlContext.read.format("com.databricks.spark.csv").option("header", true).load(moviesFile)
    val moviesDF = df2.select(df2.col("movieId"), df2.col("title"), df2.col("genres"))
    moviesDF.show(false)

    //registering dataframes to make queries
    ratingsDF.createOrReplaceTempView("ratings")
    moviesDF.createOrReplaceTempView("movies")

    //explore the dataset of movielens
    val numRatings = ratingsDF.count()
    val numUsers = ratingsDF.select(ratingsDF.col("userId")).distinct().count()
    val numMovies = ratingsDF.select(ratingsDF.col("movieId")).distinct().count()
    println("Got " + numRatings + " ratings from " + numUsers + " users on " + numMovies + " movies.")

    //quering maxumum and min rating along with count
    val results = sqlContext.sql("select movies.title, movierates.maxr, movierates.minr, movierates.cntu "

      + "from(SELECT ratings.movieId,max(ratings.rating) as maxr,"

      + "min(ratings.rating) as minr,count(distinct userId) as cntu "

      + "FROM ratings group by ratings.movieId) movierates "

      + "join movies on movierates.movieId=movies.movieId "

      + "order by movierates.cntu desc")
    results.show(false)

    //find the most active users and their ratings
    val mostActiveUsersSchemaRDD = sqlContext.sql("SELECT ratings.userId, count(*) as ct from ratings "+ "group by ratings.userId order by ct desc limit 10")
    mostActiveUsersSchemaRDD.show(false)

    //spliting the data into traingning and test data set
    val splits = ratingsDF.randomSplit(Array(0.75, 0.25), seed = 12345L)

    val (trainingData, testData) = (splits(0), splits(1))

    val numTraining = trainingData.count()

    val numTest = testData.count()

    println("Training: " + numTraining + " test: " + numTest)


    //calculating the cosine similarity differences

  }
}