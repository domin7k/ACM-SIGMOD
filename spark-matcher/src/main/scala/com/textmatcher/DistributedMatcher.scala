package com.textmatcher

import org.apache.spark.sql.SparkSession
import scala.collection.mutable.{ArrayBuffer, Queue, Map => MutableMap}

case class MatchResult(docId: Int, resultIds: Seq[Int]) extends Serializable
case class QueryInfo(queryId: Int, queryStr: String, matchType: Int, matchDist: Int) extends Serializable

class DistributedMatcher(spark: SparkSession) extends Serializable {
  @transient private val queries = MutableMap[Int, QueryInfo]()
  @transient private val documentBuffer = new ArrayBuffer[(Int, String)]()
  @transient private val results = new Queue[MatchResult]()
  private val batchSize = 100
  @transient private var resultIterator: Iterator[MatchResult] = Iterator.empty

  def startQuery(queryId: Int, queryStr: String, matchType: Int, matchDist: Int): Unit = {
    queries(queryId) = QueryInfo(queryId, queryStr, matchType, matchDist)
  }

  def endQuery(queryId: Int): Unit = {
    queries.remove(queryId)
  }

  def matchDocument(docId: Int, content: String): Unit = {
    documentBuffer.append((docId, content))
    if (documentBuffer.size >= batchSize) {
      processDocuments()
    }
  }

  def processDocuments(): Unit = {
    if (documentBuffer.isEmpty) return
    
    val currentDocs = documentBuffer.toSeq
    documentBuffer.clear()
    
    if (queries.isEmpty) {
      resultIterator = Iterator.empty
      return
    }

    val queriesBC = spark.sparkContext.broadcast(queries.toMap)
    try {
      val docsRDD = spark.sparkContext.parallelize(currentDocs)
      val results = docsRDD.map { case (docId, content) =>
        val matches = queriesBC.value.collect { 
          case (queryId, query) if isMatch(content, query) => queryId 
        }
        if (matches.nonEmpty) Some(MatchResult(docId, matches.toList.sorted))
        else None
      }.collect().flatten
      
      resultIterator = results.iterator
    } finally {
      queriesBC.unpersist(blocking = true)
    }
  }

  private def isMatch(content: String, query: QueryInfo): Boolean = {
    query.matchType match {
      case 0 => content == query.queryStr
      case 1 => hammingDistance(content, query.queryStr) <= query.matchDist
      case 2 => editDistance(content, query.queryStr) <= query.matchDist
    }
  }

  def getNextResult(): Option[MatchResult] = {
    if (!resultIterator.hasNext && documentBuffer.nonEmpty) {
      processDocuments()
    }
    if (resultIterator.hasNext) Some(resultIterator.next())
    else None
  }

  private def hammingDistance(s1: String, s2: String): Int = {
    if (s1.length != s2.length) Int.MaxValue
    else s1.zip(s2).count { case (c1, c2) => c1 != c2 }
  }

  private def editDistance(str1: String, str2: String): Int = {
    val m = str1.length
    val n = str2.length
    val dp = Array.ofDim[Int](m + 1, n + 1)

    for (i <- 0 to m) dp(i)(0) = i
    for (j <- 0 to n) dp(0)(j) = j

    for {
      i <- 1 to m
      j <- 1 to n
    } {
      val cost = if (str1(i - 1) == str2(j - 1)) 0 else 1
      dp(i)(j) = (dp(i - 1)(j) + 1) min (dp(i)(j - 1) + 1) min (dp(i - 1)(j - 1) + cost)
    }
    dp(m)(n)
  }
}