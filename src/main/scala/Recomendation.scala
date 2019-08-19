import java.util.Optional

import breeze.linalg.min
import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.log4j.Logger
import org.apache.log4j.Level
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

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    //spark session
    val spark = SparkSession
      .builder
      .appName("Video Recomendation system")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._

    //    val ratings = spark.read.textFile("gs://dataproc-aedcaf69-2bf5-4f15-9a1c-999989fa8805-asia-southeast1/sample_movielens_ratings.txt")
    val ratings = spark.read.textFile("data/sample_movielens_ratings_small.txt")
      .map(parseRating)
      .toDF().cache()
    ratings.createOrReplaceTempView("ratings")
    // Summarize ratings
      val ratingTuples = ratings.map(showRating)

    val brodcastRatings = spark.sparkContext.broadcast(ratingTuples.collect())

    val movies = ratingTuples.map(_._2).distinct().sort($"value").cache()
    var moviesDiff = ArrayBuffer[(Int,Array[(Int, Int, Double)])]()
    val moviesColl = movies.collect()
    for (mov <- moviesColl){
      val ratingsCalllected = ratingTuples.collect()
      val users = ratingTuples.filter(rat => rat._2.equals(mov))

      for (user <- users.collect()){
        val userMovies = ratingTuples.filter(rat => rat._1.equals(user._1))
        var userRat = userMovies.filter(rat => rat._2.equals(mov)).take(1)
        var currentUserRating = (0, 0, 0F)
        for (rat <- userRat){
          currentUserRating = rat
        }
//        println(currentUserRating)
        val similarMovies = userMovies.rdd.map( rat => {
          var diff : (Int, Int, Double) = null
          if(!rat._2.equals(currentUserRating._2)){
            diff = (rat._1,rat._2,math.pow(currentUserRating._3-rat._3,2))
//            diff : (Int, Int, Float)
          }
          if(diff != null)
            diff : (Int, Int, Double)
          else
            null
        })
        moviesDiff.append((currentUserRating._2,similarMovies.collect()))
      }

    }

    val moviesDiffRdds = spark.sparkContext.parallelize(moviesDiff)
    println("movies size: "+moviesDiffRdds.count())
    println("out of big loop")
    val equiDistanceValues = ArrayBuffer[Array[(Int,Int,Double)]]()
    moviesColl.foreach(movId => {
      val movieIdDiffs = moviesDiffRdds.filter(movRat => movRat._1.equals(movId))
      var combinedMovies = ArrayBuffer[(Int,Int,Double)]()
      for(mov <- movieIdDiffs.collect()){
        for( mo <- mov._2){
          if(mo != null) {
            val prev = combinedMovies.filter(_._2.equals(mo._2)).take(1)
            var position = -1
            if(prev.nonEmpty){
              position = combinedMovies.indexOf(prev(0))
            }
            if(position != -1){
              val previousDiff = combinedMovies.remove(position)
              combinedMovies.prepend((movId,previousDiff._2,previousDiff._3+mo._3))
            }else{
              combinedMovies.prepend((movId,mo._2, mo._3))
            }
          }
        }
      }
//      combinedMovies.foreach(println)
      val combinedMoviesRdds = spark.sparkContext.parallelize(combinedMovies)
      equiDistanceValues.prepend(combinedMoviesRdds.map(rating => (rating._1,rating._2,Math.sqrt(rating._3))).collect())
    })

    val firstMovies = moviesColl(0)
    val equiDistanceValuesRdds = spark.sparkContext.parallelize(equiDistanceValues)
    val firstMovieBrodcasted = spark.sparkContext.broadcast(firstMovies)
    val firstMoviesDistanceRdds = equiDistanceValuesRdds.map(mov => {
        var currentDistance : (Int, Int, Double) = null
        for (m <- mov){
          if(m._1.equals(firstMovieBrodcasted.value) || m._2.equals(firstMovieBrodcasted.value))
            if(currentDistance == null){
              currentDistance = m
            }else{
              if(m._3 < currentDistance._3)
                currentDistance = m
            }
        }
        currentDistance
    })
    var sortedDistancesValues = ArrayBuffer[(Int,Int,Double)]()
    for(distances <- equiDistanceValues){
      for(distance <- distances){
        sortedDistancesValues.append(distance)
      }
    }

//    sortedDistancesValues.foreach(println)
    var sortedDistancesValuesRdds = spark.sparkContext.parallelize(sortedDistancesValues)
    val firstrating = firstMoviesDistanceRdds.collect()(0)
    val liknessCombinations = ArrayBuffer[(Int,Int,Double)]()
    liknessCombinations.append(firstrating)
    var count = 0
    while (count < liknessCombinations.size){
      val currentRating = liknessCombinations(count)
      println(currentRating)
//      val currentRatingBrodCasted = spark.sparkContext.broadcast(currentRating)

      val allEquiDistRatings = sortedDistancesValuesRdds.filter(mov => mov._2.equals(currentRating._2) || mov._1.equals(currentRating._2))
      println(allEquiDistRatings.count())
      if(allEquiDistRatings.count() > 0){
        val finalDistance = allEquiDistRatings.reduce((x, y) => if(x._3 < y._3) x else y)
//        allEquiDistRatings.unpersist()
        liknessCombinations.append(finalDistance)
        val finalDistanceRdd = spark.sparkContext.parallelize(Array(finalDistance))
          println("final Distance : "+finalDistance)
        sortedDistancesValuesRdds = sortedDistancesValuesRdds.subtract(finalDistanceRdd)
//        println(sortedDistancesValuesRdds.count())
//        finalDistanceRdd.unpersist()
      }
      println("likness size: "+liknessCombinations.size)
      println("Looop count: "+count)
      count +=1

    }

    sortedDistancesValuesRdds.collect().foreach(println)

  }

}
