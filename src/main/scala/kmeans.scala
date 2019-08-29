import Recomendation.{parseRating, showRating}
import org.apache.log4j.{Level, Logger}
import org.apache.spark
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
    val fields = str.split("::")
    assert(fields.size == 4)
    (fields(0).toInt, fields(1).toInt, fields(2).toFloat)
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
val ratings = spark.read.textFile("data/sample_movielens_ratings.txt")
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
    completeRatings.foreach(x=> {
      println(x._1)
      x._2.foreach(println)
    })

    val denseVectors = spark.sparkContext.parallelize(completeRatings).map(ratings => {
      val ratingArray = ArrayBuffer[Double]()
      ratings._2.foreach(rating=> {
        ratingArray.append(rating._3)
      })
      val vector = Vectors.dense(ratingArray.toArray)
      vector
    })
    denseVectors.foreach(x=>{
      x.toArray.foreach(println)
    })

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

//    users.foreach(user => {
//      println(user)
//      val userMovies = ratingTuples.filter( rating => rating._1 == user).sort($"_2")
//      val movieIdAccum = spark.sparkContext.accumulator(0, "movie Id accumulator")
//      userMovies.map(movie => {
//
//      })
//    })


//    users.foreach(println)
//
//  val test =  training.rdd.map(r => {
//   val vector = Vectors.dense(r._1.toDouble, r._2.toDouble,r._3.toDouble)
//
//    vector : linalg.Vector
// })
//
//    // Cluster the data into two classes using KMeans
//    val numClusters = 10
//    val numIterations = 40
//    val clusters = KMeans.train(test, numClusters, numIterations)
//    // Evaluate clustering by computing Within Set Sum of Squared Errors
//    val WSSSE = clusters.computeCost(test)
//    println(s"Within Set Sum of Squared Errors = $WSSSE")
////    println(clusters.clusterCenters)
////    println(clusters.distanceMeasure)
////    println(clusters.predict(tra))
//    clusters.clusterCenters.foreach(
//      center => {
//        println(center)
//      }
//    )
//
//    val parsedTestData =  testData.rdd.map(r => {
//      val vector = Vectors.dense(r._1.toDouble, r._2.toDouble,r._3.toDouble)
//
//      vector : linalg.Vector
//    })
//
//    clusters.predict(parsedTestData).foreach(println)
//
//    // Save and load model
//    clusters.save(spark.sparkContext, "target/org/apache/spark/KMeansExample/KMeansModel")
//    val sameModel = KMeansModel.load(spark.sparkContext, "target/org/apache/spark/KMeansExample/KMeansModel")
//

  }
}

case class A(features: org.apache.spark.ml.linalg.Vector)