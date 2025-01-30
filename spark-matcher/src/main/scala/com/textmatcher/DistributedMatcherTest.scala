package com.textmatcher

import org.apache.spark.sql.SparkSession

object DistributedMatcherTestMain {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("TextMatcherTest")
      .master("spark://513f6468f9ee:7077")
      .getOrCreate()

    val matcher = new DistributedMatcher(spark)

    var testResults = Map[String, Boolean]()  // Map zum Speichern der Testergebnisse

    try {
      // Test 1: Exact Matches
      println("Running test: handle exact matches")
      matcher.startQuery(1, "hello world", 0, 0)
      matcher.matchDocument(1, "hello world")
      var result = matcher.getNextResult()
      println(s"Exact match result: $result")
      val exactMatchResult = result.isDefined && result.get == MatchResult(1, Seq(1))
      testResults += ("handle exact matches" -> exactMatchResult)
      assert(exactMatchResult)

      // Test 2: Hamming Distance Matches
      println("Running test: handle Hamming distance matches")
      matcher.startQuery(2, "hello", 1, 1)
      matcher.matchDocument(2, "hallo")
      result = matcher.getNextResult()
      println(s"Hamming distance match result: $result")
      val hammingDistanceResult = result.isDefined && result.get == MatchResult(2, Seq(2))
      testResults += ("handle Hamming distance matches" -> hammingDistanceResult)
      assert(hammingDistanceResult)

      // Test 3: Edit Distance Matches
      println("Running test: handle edit distance matches")
      matcher.startQuery(3, "hello", 2, 2)
      matcher.matchDocument(3, "helo")
      result = matcher.getNextResult()
      println(s"Edit distance match result: $result")
      val editDistanceResult = result.isDefined && result.get == MatchResult(3, Seq(3))
      testResults += ("handle edit distance matches" -> editDistanceResult)
      assert(editDistanceResult)

      // Test 4: Batch Process Documents
      println("Running test: batch process documents")
      matcher.startQuery(4, "test", 0, 0)
      for (i <- 1 to 150) {
        matcher.matchDocument(i, "test")
      }
      var count = 0
      while (matcher.getNextResult().isDefined) {
        count += 1
      }
      println(s"Batch process result count: $count")
      val batchProcessResult = count == 150
      testResults += ("batch process documents" -> batchProcessResult)
      assert(batchProcessResult)

    } catch {
      case e: Exception =>
        println(s"Test failed with exception: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      // Ausgabe der Zusammenfassung der Testergebnisse
      println("\nTest Summary:")
      testResults.foreach {
        case (testName, passed) =>
          if (passed) {
            println(s"$testName: PASSED")
          } else {
            println(s"$testName: FAILED")
          }
      }

      // Gesamtstatus
      if (testResults.values.forall(_ == true)) {
        println("\nAll tests passed successfully!")
      } else {
        println("\nSome tests failed. Please check the above results for details.")
      }

      spark.stop()
    }
  }
}
