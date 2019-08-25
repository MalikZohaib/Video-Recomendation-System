import java.util.Optional

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.collection.mutable.{ArrayBuffer}


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


  def calculateMoviesDiff(moviesColl : Array[Int], ratingTuples: Dataset[(Int,Int,Float)]) : ArrayBuffer[(Int,Array[(Int,Int,Double)])] = {
    var moviesDiff = ArrayBuffer[(Int,Array[(Int, Int, Double)])]()
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
    return moviesDiff
  }

  def calculateEquiDistance (moviesColl : Array[Int], moviesDiffRdds : RDD[(Int, Array[(Int,Int,Double)])]): ArrayBuffer[Array[(Int,Int,Double)]] = {
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
      //      val combinedMoviesRdds = spark.sparkContext.parallelize(combinedMovies)
      equiDistanceValues.prepend(combinedMovies.toArray)
    })
    return equiDistanceValues
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

//        val ratings = spark.read.textFile("gs://dataproc-aedcaf69-2bf5-4f15-9a1c-999989fa8805-asia-southeast1/sample_movielens_ratings.txt")
    val ratings = spark.read.textFile("data/sample_movielens_ratings_small.txt")
      .map(parseRating)
      .toDF().cache()
    ratings.createOrReplaceTempView("ratings")
    // Summarize ratings
      val ratingTuples = ratings.map(showRating)

    val brodcastRatings = spark.sparkContext.broadcast(ratingTuples.collect())

    val movies = ratingTuples.map(_._2).distinct().sort($"value").cache()
    val moviesColl = movies.collect()

    //calculate Movies Diff
    val moviesDiff = calculateMoviesDiff(moviesColl, ratingTuples)

    val moviesDiffRdds = spark.sparkContext.parallelize(moviesDiff)


    println("out of big loop")

    //calculating  Equidistance values
    val equiDistanceValues = calculateEquiDistance(moviesColl,moviesDiffRdds)

    //getting first movie Rdd values
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

    //sort equidistance Values
    var sortedDistancesValues = ArrayBuffer[(Int,Int,Double)]()
    for(distances <- equiDistanceValues){
      for(distance <- distances){
        sortedDistancesValues.append(distance)
      }
    }

    var sortedDistancesValuesRdds = spark.sparkContext.parallelize(sortedDistancesValues)
    val groupedData = moviesColl.map(movId => {
      val group = sortedDistancesValuesRdds.filter(dist => dist._2.equals(movId) || dist._1.equals(movId))
      val moveIdBroadCasted = spark.sparkContext.broadcast(movId)
      val groupSorted = group.map(dist => {

        var distance : (Int, Int, Double) = null
        if(dist._2.equals(moveIdBroadCasted.value) || dist._1.equals(moveIdBroadCasted.value)){
          if(dist._1.equals(moveIdBroadCasted.value)){
            distance = (moveIdBroadCasted.value,dist._2,dist._3)
          }else{
            distance = (moveIdBroadCasted.value,dist._1,dist._3)
          }
        }
//        if(distance != null)
          distance
      }).collect()


      (movId, groupSorted.distinct)
    })

    println("calculating Likness")
    var cleanedGroupedDataRdds = spark.sparkContext.parallelize(groupedData)
    val firstrating = firstMoviesDistanceRdds.collect()(0)
    val liknessCombinations = ArrayBuffer[(Int,Int,Double)]()
    liknessCombinations.append(firstrating)
    var count = 0
    while (count < liknessCombinations.size){
      val currentRating = liknessCombinations(count)
      println("current rating:" +currentRating)
      //      val currentRatingBrodCasted = spark.sparkContext.broadcast(currentRating)

      val allEquiDistRatings = cleanedGroupedDataRdds.filter(mov => mov._1.equals(currentRating._2))
//      allEquiDistRatings.foreach(x => {
//        println(x._1)
//        x._2.foreach(println)
//      })
//      allEquiDistRatings.foreach(x=>x._2.foreach(println))
      println("All Equi distance size: "+ allEquiDistRatings.count())
      if(allEquiDistRatings.count() > 0){
        var allDistancesRdds = spark.sparkContext.parallelize(allEquiDistRatings.first()._2)
//        allEquiDistRatings.collect()(0)._2.foreach(println)
        println(allEquiDistRatings.collect()(0)._1)
        if(allDistancesRdds.count() > 0){
          var finalDistance = allDistancesRdds.reduce((x, y) => if(x._3 < y._3) x else y)
          //        while(currentRating.equals(finalDistance) || (currentRating._1.equals(finalDistance._2) && currentRating._2.equals(finalDistance._1))){
          //          val remainingDistances = allDistancesRdds.subtract(spark.sparkContext.parallelize(Array(finalDistance)))
          //          finalDistance = remainingDistances.reduce((x, y) => if(x._3 < y._3) x else y)
          //        }

          //        allEquiDistRatings.unpersist()
          liknessCombinations.append(finalDistance)
          val newEquiDistanceValues = allDistancesRdds.filter(dist => !dist.equals(finalDistance)).collect()
          println("allequidistance: "+allEquiDistRatings.count() )
          println("Size of groupData:"+cleanedGroupedDataRdds.count())
          cleanedGroupedDataRdds = cleanedGroupedDataRdds.filter(dist => !dist._1.equals(finalDistance._1))
          println("Size of groupData:"+cleanedGroupedDataRdds.count())
//          val newEquiDistanceValuesRdds = spark.sparkContext.parallelize(ArrayBuffer)
          if(newEquiDistanceValues.length > 0){
            val cleanedGroup = ArrayBuffer[(Int,Array[(Int,Int,Double)])]()
            cleanedGroup.appendAll(cleanedGroupedDataRdds.collect())
            cleanedGroup.append((finalDistance._1, newEquiDistanceValues))
            cleanedGroupedDataRdds = spark.sparkContext.parallelize(cleanedGroup)
            //          cleanedGroupedDataRdds ++ cleanedGroupedDataRdds.union(newEquiDistanceValuesRdds.first())
            println("Size of groupData:"+cleanedGroupedDataRdds.count())
            //          val finalDistanceRdd = spark.sparkContext.parallelize( Array((currentRating._2, Array(finalDistance))))
            println("final Distance : "+finalDistance)
            //        groupedDataRdds = groupedDataRdds.subtract(finalDistanceRdd)
          }

        }

      }
      println("likness size: "+liknessCombinations.size)
      count +=1
      println("Looop count: "+count)
    }

    liknessCombinations.foreach(println)

    val parsedData =  spark.sparkContext.parallelize(liknessCombinations).map(r => {
      val vector = Vectors.dense(r._3.toDouble)

      vector : linalg.Vector
    })

    // Cluster the data into two classes using KMeans
    val numClusters = 10
    val numIterations = 40
    val clusters = KMeans.train(parsedData, numClusters, numIterations)
    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    println(s"Within Set Sum of Squared Errors = $WSSSE")
    //    println(clusters.clusterCenters)
    println(clusters.distanceMeasure)
    //    println(clusters.predict(tra))
    val scenters = ArrayBuffer[Double]()
     clusters.clusterCenters.foreach( x => {
       scenters.append(x.toArray(0))
    })
    val sortedCenters = scenters.sortWith(_ < _)
    sortedCenters.foreach(println)

    spark.sparkContext.parallelize(liknessCombinations).map(likness => {
      var prevDistance : Double = -1
      var prevCluster = sortedCenters(0)
      var found = false
      var count = 0
      while(!found && count+1 <= sortedCenters.size){
        val cluster = sortedCenters(count)
        val newDistance = Math.sqrt(Math.pow(likness._3-cluster,2))
        if(prevDistance != -1){
          if(newDistance > prevDistance){
            found=true
          } else {
            prevCluster = cluster
            prevDistance = newDistance
          }
        }else {
          prevCluster = cluster
          prevDistance = newDistance
        }
        count += 1
      }
//      val predictedDistance = sortedCenters.foreach(cluster => {
//        val newDistance = Math.sqrt(Math.pow(likness._3-cluster,2))
//        if(prevDistance != -1){
//          if(newDistance > prevDistance){
//            prevDistance
//          } else {
//            prevCluster = cluster
//            prevDistance = newDistance
//          }
//        }else {
//          prevCluster = cluster
//          prevDistance = newDistance
//        }
//        prevCluster = cluster
//      })
//      if(predictedDistance == prevDistance)
      (prevCluster,likness._1,likness._2,likness._3)
    }).collect().groupBy(x => x._1).toArray.foreach(x => {
      println(x._1)
      x._2.foreach(println)
    })

    //testing user rating
    val testUserRatings = Array((0,1,3.0),(0,2,4.0),(0,5,3.0))
    calcualteSimilarityForUser(testUserRatings)


//    println(sortedCenters)


    // Save and load model
    clusters.save(spark.sparkContext, "data/KMeansExample/KMeansModel")
    val sameModel = KMeansModel.load(spark.sparkContext, "data/KMeansExample/KMeansModel")


  }

  def calcualteSimilarityForUser (movies : Array[(Int, Int, Double)]): Unit = {
    val similarities = ArrayBuffer((Int, Int, Double))
    for (mov <- movies){
        for(movies <- movies){

        }
    }
  }

}
