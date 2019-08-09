import org.apache.spark
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.broadcast
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}


object Recomendation {

  def parseRating(str: String): Tuple3[Int, Int, Float] = {
    val fields = str.split("::")
    assert(fields.size == 4)
    (fields(0).toInt, fields(1).toInt, fields(2).toFloat)
  }
//
  def showRating(row: Row) : Tuple3[Int, Int, Float] = {
    (row.getAs(0),row.getAs(1),row.getAs(2))
  }

  def main(args: Array[String]): Unit = {

    //spark session
    val spark = SparkSession
      .builder
      .appName("Video Recomendation system")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._

    val ratings = spark.read.textFile("data/sample_movielens_ratings.txt")
      .map(parseRating)
      .toDF().cache()
    ratings.createOrReplaceTempView("ratings")
    // Summarize ratings
      val ratingTuples = ratings.map(showRating)

    val brodcastRatings = spark.sparkContext.broadcast(ratingTuples.collect())

//    ratings.map{rating =>
////      print(rating(0))
//      Tuple3(rating(0),rating(1),rating(2))
//    }
//    ratingTuples.collect()
    val movies = ratingTuples.map(_._2).distinct().sort($"value").cache()
    for (mov <- movies.collect()){
      val users = ratingTuples.collect().filter(rat => rat._2.equals(mov))

    }
//    ratingTuples.show()
//    ratingTuples.collect()
//    val movIds = movies.map( mov => {
//      var users = ArrayBuffer[(Int, Int, Float)]
//      for( rating <-  brodcastRatings.value ){
//        if(rating._2.equals(mov)){
//          users.apply(Seq(rating))
//        }
//      }
//  val usrs = brodcastRatings.value.foreach(rating => {
//        if(mov.equals(rating._2)){
//          return rating : (Int, Int, Float)
//        }
//      })
//  users
//    })
//    movIds.show(100)
//    var users = ArrayBuffer[(Int,Int,Float)]
//      val cosineSimilarities = movies.map(movie => {
////      val users = spark.sql(s"select * from ratings where _2=${movie.toInt}")
//    var users = ArrayBuffer()
//      ratingTuples.foreach( mov => {
//        if(mov._2 == movie){
//          users.compose(Seq(mov._1.toInt, mov._2.toInt, mov._3.toFloat))
//        }
//      })
//
//      users
////      val users = ratings.selectExpr()
////      ratings.map( (rating) => {
////        if(rating.getAs(1) == movie){
////          val user = rating.getAs(0)
////
////        }
////      })
////      users.collect().length: Int
//    })
//    cosineSimilarities.show()
    //    movies.show(100)
//    groupByMovies.collect()

//    usersGroup.agg("_2")
//    usersGroup.agg()
//    groupByMovies.show(100)
//    val movies =   groupByMovies.toDF()

//    println(usersGroup.show())
//    val processedTuples = ratingTuples.map(rat => {
////      val recomendation = ratingTuples.map(x  => {
//////          if(rat.getAs(0) == x.getAs(0)){
////            println("yes")
//////          }
////         x._1 : Int
////      })
////      recomendation.show()
//      rat._1 : Int
//    })
//    ratingTuples.show(100)
//    processedTuples.show()
//      ratings.map{x=>
//        print(x.getAs("user").toString())
//        (x.getAs("user"), )
//      }
//      val cosineSimalarities = ratings.map(r => showRating(r.getAs("user").toString(),r.getAs("product").toString(), r.getAs("rating").toString()))
//        cosineSimalarities.count()
    // Map ratings to 1 or 0, 1 indicating a movie that should be recommended
//    val binarizedRatings = ratings.map(r => Rating(r.getAs("user"), r.getAs("product"),r.getAs("rating"))
    // Summarize ratings
//    val numRatings = ratings.count()
//    val numUsers = ratings.map(_.getAs("user").toString()).distinct().count()
//    val numMovies = ratings.map(_.getAs("product").toString()).distinct().count()
//    println(s"Got $numRatings ratings from $numUsers users on $numMovies movies.")

//    val splits = ratings.randomSplit(Array(0.8, 0.2))
//    val training = splits(0).cache()
//
//    val numTraining = training.count()
//    println(s"Training: $numTraining")
//    val numRatings = ratings.count()
//    val numUsers = ratings.map(_.user).distinct().count()
//    val numMovies = ratings.map(_.product).distinct().count()
//    println(s"Got $numRatings ratings from $numUsers users on $numMovies movies. " )
////    ratings.map(r => showRating(r.user,r.product,r.rating))
//    val sqlContext = spark.sqlContext
    //spark context
//    val conf:SparkConf = new SparkConf().setAppName("Video Recomendation system").setMaster("local")
//    val sc:SparkContext = new SparkContext(conf)
//    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    //rating file
//    val ratigsFile = "data/ratings.csv"
//    val df1 = spark.read.format("com.databricks.spark.csv").option("header", true).load(ratigsFile)
//
//    val ratingsDF = df1.select(df1.col("userId"), df1.col("movieId"), df1.col("rating"), df1.col("timestamp"))
//    ratingsDF.show(false)

    //movie file
//    val moviesFile = "data/movies.csv"
//    val df2 = spark.read.format("com.databricks.spark.csv").option("header", true).load(moviesFile)
//    val moviesDF = df2.select(df2.col("movieId"), df2.col("title"), df2.col("genres"))
//    moviesDF.show(false)

    //registering dataframes to make queries
//    ratingsDF.createOrReplaceTempView("ratings")
//    moviesDF.createOrReplaceTempView("movies")

//    //explore the dataset of movielens
//    val numRatings = ratingsDF.count()
//    val numUsers = ratingsDF.select(ratingsDF.col("userId")).distinct().count()
//    val numMovies = ratingsDF.select(ratingsDF.col("movieId")).distinct().count()
//    println("Got " + numRatings + " ratings from " + numUsers + " users on " + numMovies + " movies.")
//
//    //quering maxumum and min rating along with count
//    val results = spark.sql("select movies.title, movierates.maxr, movierates.minr, movierates.cntu "
//
//      + "from(SELECT ratings.movieId,max(ratings.rating) as maxr,"
//
//      + "min(ratings.rating) as minr,count(distinct userId) as cntu "
//
//      + "FROM ratings group by ratings.movieId) movierates "
//
//      + "join movies on movierates.movieId=movies.movieId "
//
//      + "order by movierates.cntu desc")
//    results.show(false)
//
//    //find the most active users and their ratings
//    val mostActiveUsersSchemaRDD = spark.sql("SELECT ratings.userId, count(*) as ct from ratings "+ "group by ratings.userId order by ct desc limit 10")
//    mostActiveUsersSchemaRDD.show(false)

    //spliting the data into traingning and test data set
//    val splits = ratingsDF.randomSplit(Array(0.75, 0.25), seed = 12345L)
//
//    val (trainingData, testData) = (splits(0), splits(1))

//    val numTraining = trainingData.count()
//
//    val numTest = testData.count()
//
//    println("Training: " + numTraining + " test: " + numTest)
//    val rdds = trainingData.toDF()
//    println(getTheAnswer())
//  def doStuff(rdd: String): RDD[String] = {
////    val field_ = this.field
////    rdd.map(x => field_ + x)
//    return rdd
//  }

    //calculating the cosine similarity differences
//    val newT = new MyTokenlizer()
//    trainingData.map (x => println(x))


  }

}
//
//class MyTokenlizer extends Serializable {
//
//  def getRows(CurRow:Row):Row={
//
////    val somefield =curRow.getAs[String]("field1")
////
////    --- saome manipulation happening here and finally return a array of rows
////
////    return res[Row]
//    return CurRow
//  }
//}
