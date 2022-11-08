package statisticsForElonMuskTweets

import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender
import org.apache.spark.sql.SparkSession
import scala.io.StdIn.readLine

object statistics {

  def main(args: Array[String]): Unit = {
    val nullAppender = new NullAppender
    BasicConfigurator.configure(nullAppender)
    val spark = SparkSession.builder().master("local[4]").appName("elon musk tweets statistics").getOrCreate()

    //read tweets (RDD)
    val listOfTweetsRDD = spark.read.csv("src/elonmusk_tweets.csv").rdd

    //read keywords
    print("Enter the keywords (splitting by ',') : ")
    val ListOfKeywords = readLine().split(",")
    println("*-----------------------------------------------------------------*\n")

    //some declarations
    val numOfTweets = listOfTweetsRDD.count()


    //percentage of tweets that have at least one or two of input keywords.
    println("*-----------------------------------------------------------------*")
    def getCountOfWords(word:String,listOfKeywords:Array[String]):Int= {
      //number of keywords in tweet
      var k = 0
      word.split(" ").foreach(io=>{if(listOfKeywords contains(io))k+=1})
      k
    }
    listOfTweetsRDD.map(io=>(io(2).toString,()))
      .map(io=>(getCountOfWords(io._1,ListOfKeywords),0))
      .map(io=>("Any",({if (io._1 >= 1) 1 else 0},
        {if(io._1 <= 2 && io._1 >= 0 )1 else 0})))

      .reduceByKey((x,y)=>(x._1.toInt +  y._1.toInt, x._2.toInt + y._2.toInt))
      .map(io=>(io._2._1/numOfTweets.toFloat,io._2._2/numOfTweets.toFloat))
      .map(io=>
        "Tweets at least one input keywords : "+io._1 * 100 + "%" +
          "\n" +
          "Tweets at least two input keywords : "+ io._2 * 100 + "%")
      .foreach(println(_))

    //Average
    println("*-----------------------------------------------------------------*\n"+
      "average = " +
      listOfTweetsRDD.map(line=>(line(2).toString.split(" ").length)).sum() /
      listOfTweetsRDD.map(line=>(line(2).toString.split(" ").length)).count()
    )

    //Standard deviation
    println("*-----------------------------------------------------------------*\n"+
      "standard deviation = " +
      listOfTweetsRDD.map(line=>(line(2).toString.split(" ").length)).stdev()
    )

  }
}
