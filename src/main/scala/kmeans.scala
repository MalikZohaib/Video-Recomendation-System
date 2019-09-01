import Recomendation.{parseRating, showRating}
import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Try

object kmeans {

  def parseRating(str: String): Tuple3[Int, Int, Float] = {
    val fields = str.split(",")
    assert(fields.size == 4)
    (fields(0).toInt, fields(1).toInt, fields(2).toFloat)
  }

  def parseMovies(str: String): Tuple3[Int, String, String] = {
    val fields = str.split(",")
//    assert(fields.size == 3 || fields.size == 4)
    (fields(0).toInt, fields(1), fields(2))
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
//        moviesDiff.insert(currentUserRating._2,similarMovies.collect())
    //spark session
    val spark = SparkSession
      .builder
      .appName("Video Recomendation system")
      .config("spark.master", "local")
      .getOrCreate()
    import spark.implicits._

    val ratigsFile = "data/ratings.csv"
//    val df1 = spark.read.format("com.databricks.spark.csv").option("header", true).load(ratigsFile)
//
//    df1.show(100)
//val ratings = spark.read.textFile("data/sample_movielens_ratings.txt")
val ratings = spark.read.textFile("data/ratings.csv")
  .map(parseRating)
  .toDF().cache()
    ratings.createOrReplaceTempView("ratings")
    // Summarize ratings
    val ratingTuples = ratings.map(showRating)

//    val splits = ratingTuples.randomSplit(Array(0.8, 0.2))
//    val training = splits(0).cache()
//    val testData = splits(1).cache()
//
//    val numTraining = training.count()
//    println(s"Training: $numTraining")

    val users = ratingTuples.map( x=> x._1).sort($"value").distinct().collect()
    val movies = ratingTuples.map(_._2).distinct().sort($"value").cache()
    val moviesColl = movies.collect()
    val movieSizeBroadcasted = spark.sparkContext.broadcast(moviesColl.length)
    val completeRatings = ratingTuples.rdd.groupBy(x => x._1).map(ratings => {
      val newRating = ArrayBuffer[(Int, Int, Float)]()
      for( movieId <- 0 until  movieSizeBroadcasted.value){
        val rating = ratings._2.filter(x => x._2.equals(movieId)).take(1)
        if(rating.nonEmpty){
          rating.foreach(x=> {
            newRating.append(x)
          })

        }else{
          newRating.append((ratings._1,movieId,0))
        }
      }
      (ratings._1,newRating)
    }).collect()
//    completeRatings.foreach(x=> {
//      println(x._1)
//      x._2.foreach(println)
//    })
    val completeRatingsRdds = spark.sparkContext.parallelize(completeRatings)
    val denseVectors = completeRatingsRdds.map(ratings => {
      val ratingArray = ArrayBuffer[Double]()
      ratings._2.foreach(rating=> {
        ratingArray.append(rating._3)
      })
      val vector = Vectors.dense(ratingArray.toArray)
      vector
    })
//    denseVectors.foreach(x=>{
//      x.toArray.foreach(println)
//    })
    println("calculating K-means")
    // Cluster the data into two classes using KMeans
    val numClusters = 10
    val numIterations = 40
    val clusters = KMeans.train(denseVectors, numClusters, numIterations)
    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(denseVectors)
    println(s"Within Set Sum of Squared Errors = $WSSSE")
//    println(clusters.clusterCenters)
//    println(clusters.distanceMeasure)
//    println(clusters.predict(tra))
    clusters.clusterCenters.foreach(
      center => {
        println(center)
      }
    )

    val clustersCentersBroadCasted = spark.sparkContext.broadcast(clusters.clusterCenters)

    val usersClusters = completeRatingsRdds.map( user => {
      val equiDistances = ArrayBuffer[(Int, Double)]()

      var count = 0
      clustersCentersBroadCasted.value.foreach(cluster => {
        var diff : Double= 0.0
          for(i <- user._2.indices){
            diff += Math.pow(cluster(i) - user._2(i)._3,2)
          }
        equiDistances.append((count,Math.sqrt(diff)))
        count+=1
      })
      val minmumDistance = equiDistances.reduce((x, y) => if(x._2 < y._2) x else y)
      (user._1,minmumDistance._1,minmumDistance._2,user._2)
    })
    clustersCentersBroadCasted.unpersist()
    usersClusters.foreach(println)

    //test data generation
    //this is test data and very small amount of data so paralissiam is not neccessary
    println("=====Enter the No of movies you have watched=====")
    val noOfMovies = scala.io.StdIn.readInt()
    val r = scala.util.Random
    val randomMoviesIDs = for (i <- 1 to noOfMovies) yield r.nextInt(moviesColl.length)
    val moviesCsv = spark.read.textFile("data/movies.csv").map(parseMovies).cache()
    val testMoviesTitles = ArrayBuffer[(Int, String,String)]()
    randomMoviesIDs.foreach(movieID => {
      val firstValues = moviesCsv.filter(x => x._1.equals(movieID))
      if(firstValues.count() > 0)
        testMoviesTitles.append(firstValues.first())
    })

    //getting ratings from user
    println("Enter Ratings from 1-5 for movies")
    val userMoviesRatings = ArrayBuffer[(Int,String,Int)]()
    testMoviesTitles.foreach(movies => {
      println("Enter rating for Movies '"+movies._2+"'")
      val rating = scala.io.StdIn.readInt()
      userMoviesRatings.append((movies._1,movies._2,rating))
    })


    //creating user vector
    val userMoviesRatingsBroadCasted = spark.sparkContext.broadcast(userMoviesRatings)
    val testRatings = spark.sparkContext.parallelize(moviesColl).map(movieID => {
      val rating = userMoviesRatingsBroadCasted.value.filter( x => x._1.equals(movieID))
      if(rating.nonEmpty){
        rating(0)._3
      }else{
        0.0
      }
    }).collect()

    userMoviesRatingsBroadCasted.unpersist()

    //calculate the recomendations for user
    //calculate the closest cluster for user
    val denseVector = Vectors.dense(testRatings)
    //get the predicted cluster index for the current user
    val predictedClusterIndex = clusters.predict(denseVector)

    //calculate Person correlation
    val currentUserRatingsBroadcasted = spark.sparkContext.broadcast(userMoviesRatings)
    val clusterUsers = usersClusters.filter(cluter => cluter._2.equals(predictedClusterIndex))
    val closedUsers = clusterUsers.map(user => {
      val nonZeroRatings = user._4.filter( x=> x._3 != 0.0)
      val comparisonMovies = ArrayBuffer[(Int,Int,Float)]()
      val targetMovies = ArrayBuffer[(Int,String,Int)]()
      var sum1 = 0.0
      var sum2 = 0.0
      currentUserRatingsBroadcasted.value.foreach(rating => {
        val userRating = nonZeroRatings.filter(x => x._2.equals(rating._1))
        if(userRating.nonEmpty){
          if(!userRating(0)._3.equals(0.0)){
            comparisonMovies.append(userRating(0))
            targetMovies.append(rating)
            sum2 += userRating(0)._3
            sum1 += rating._3
          }
        }
      })

      //calculating mean
      var meanTarget = 0.0
      var meanComp = 0.0
      if(targetMovies.nonEmpty && comparisonMovies.nonEmpty){
        meanTarget = sum1/targetMovies.size
        meanComp = sum2/comparisonMovies.size
      }
      //calculate differences
      var diffSum = 0.0
      var diffSumMoviesTarget = 0.0
      var diffSumMoviesComp = 0.0
      for (i <- targetMovies.indices) {
        diffSum += (targetMovies(i)._3 - meanTarget) * (comparisonMovies(i)._3-meanComp)
        diffSumMoviesTarget += Math.pow(targetMovies(i)._3-meanTarget,2)
        diffSumMoviesComp += Math.pow(comparisonMovies(i)._3-meanComp,2)
      }

      val petersonSimilarity = diffSum/(Math.sqrt(diffSumMoviesTarget)*Math.sqrt(diffSumMoviesComp))

      (user._1,user._2,user._3,petersonSimilarity, user._4)
    }).filter(x => !x._4.isNaN).sortBy(x => x._4,ascending = false)
//    closedUsers.foreach(println)
    println("=== users similarities ===")

    if(closedUsers.count() > 0){
      val closeedUser = closedUsers.reduce((x, y) => if(x._4 > y._4) x else y)
      println("Closed User : "+closeedUser._1)
      println("Peterson Similarity : "+closeedUser._4)
      //    println(closeedUser)

      closeedUser._5.foreach(x => {
        val ratings = userMoviesRatings.filter(y => y._1==x._2 && y._3 > 0.0)
        if(ratings.isEmpty){
          val movies = moviesCsv.filter(y => y._1.equals(x._2))
          if(movies.count() > 0)
            println(movies.first())
        }
      })
    }

  }
}

case class A(features: org.apache.spark.ml.linalg.Vector)