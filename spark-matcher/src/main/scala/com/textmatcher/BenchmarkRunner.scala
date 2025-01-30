package com.textmatcher

import scala.io.Source
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.Queue

class BenchmarkRunner(spark: SparkSession) {
  private val matcher = new DistributedMatcher(spark)
  private val results = Queue[MatchResult]()

  def runBenchmark(inputFile: String): Unit = {
    val startTime = System.currentTimeMillis()
    val source = Source.fromFile(inputFile)
    
    try {
      val lines = source.getLines().toList
      
      lines.foreach { line =>
        val parts = line.trim.split("\\s+", 2)
        parts(0) match {
          case "s" =>
            val params = parts(1).split("\\s+")
            matcher.startQuery(
              params(0).toInt,
              params.drop(4).mkString(" "),
              params(1).toInt,
              params(2).toInt
            )

          case "e" =>
            matcher.endQuery(parts(1).trim.toInt)

          case "m" =>
            val params = parts(1).split("\\s+", 2)
            matcher.matchDocument(params(0).toInt, params(1))

          case "r" =>
            matcher.processDocuments()
            collectResults()
        }
      }
      
      // Process any remaining documents
      if (!lines.last.startsWith("r")) {
        matcher.processDocuments()
        collectResults()
      }
      
    } finally {
      source.close()
      printResults(startTime)
    }
  }

  private def collectResults(): Unit = {
    var result = matcher.getNextResult()
    while (result.isDefined) {
      results.enqueue(result.get)
      result = matcher.getNextResult()
    }
  }

  private def printResults(startTime: Long): Unit = {
    val endTime = System.currentTimeMillis()
    println(s"Benchmark completed in ${endTime - startTime} ms")
    println("Results:")
    results.foreach(result => 
      System.out.println(s"Document ID: ${result.docId}, Matching Query IDs: ${result.resultIds.mkString(", ")}")
    )
  }
}

object BenchmarkRunnerMain {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("Usage: BenchmarkRunnerMain <inputFile>")
      sys.exit(1)
    }

    var spark: SparkSession = null
    try {
      spark = SparkSession.builder()
        .appName("BenchmarkRunner")
        .master("spark://513f6468f9ee:7077")
        .getOrCreate()

      val runner = new BenchmarkRunner(spark)
      runner.runBenchmark(args(0))
    } finally {
      if (spark != null) {
        spark.stop()
      }
    }
  }
}