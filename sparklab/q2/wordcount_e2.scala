// Mandatory imports for Spark RDDs
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Q2 {
  /* This purpose of this program is to use the War and Peace dataset in /datasets/wap
   * to compute the variation of question 1 (only wanting words with an 'e') including 
   * the count of words that appear 2 or more times total
   * A word is any sequence of characters that has no space in it
   */

  def main(args: Array[String]): Unit = {
    val sc = getSC() //Initializes SparkContext
    val myRDD = getRDD(sc) //Loads dataset
    val myresults = doWordCount(myRDD) //Processes data to get word count
    saveit("spark2output", myresults) //Saves results to HDFS
  }

  def getSC() = {
    /* Gets the spark context variable with
     * application name "Q2 Word Count"
     */
    val conf = new SparkConf().setAppName("Q2 Word Count") //Change app name
    val sc = new SparkContext(conf)
    sc
  }

  def getRDD(sc: SparkContext): RDD[String] = { //Document the RDD
    /*
     * This is an RDD in which each entry is a string corresponding
     * to a line of text.
     */
    sc.textFile("/datasets/wap")
  }

  def getTestRDD(sc: SparkContext): RDD[String] = {
    val testlist = List(
      "the quick brown fox",
      "jumps over the lazy dog",
      "never ever fall"
    )
    sc.parallelize(testlist, 3) //Transformation, no computation
  }
  
  def doWordCount(input: RDD[String]): RDD[(String, Int)] = {
    /* The purpose of this question is to count the number of times that
     * each word appears in the input RDD 2 or more times 
     * 
     * Input: an RDD where each entry is a string, corresponding to a line of text
     * Output: an RDD where each entry is a (word, count) tuple.
     * 
     * This function works by reading the RDD, splitting the lines by whitespace, 
     * removing blank words, filters words containing "e", maps each word to 
     * (word, 1), sums up the results, and gathers words appearing more than 2 times
     */
    
    val words = input.flatMap(line => line.split("\\W+")) //Splits by whitespace
    /* RDD words:
     * "the"
     * "quick"
     * "brown"
     * "fox"
     * "jumps"
     * "over"
     * "the"
     * "lazy"
     * "dog"
     * "never"
     * "ever"
     * "fall"
     */
    val noblank = words.filter(line => line.size > 0) //Removes blank words
    /* RDD noblank:
     * "the"
     * "quick"
     * "brown"
     * "fox"
     * "jumps"
     * "over"
     * "lazy"
     * "dog"
     * "never"
     * "ever"
     * "fall"
     */
    val ewords = noblank.filter(word => word.contains("e")) //Filters words with "e"
    /* RDD ewords:
     * "the"
     * "over"
     * "never"
     * "ever"
     */
    val kv = ewords.map(word => (word, 1)) //Maps each word to (word, 1)
    /* RDD kv:
     * ("the", 1)
     * ("over", 1)
     * ("never", 1)
     * ("ever", 1)
     */
    val myresults = kv.reduceByKey((count1, count2) => count1 + count2) //Sums results
    /* RDD myresults:
     * ("the", 2)
     * ("over", 1)
     * ("never", 1)
     * ("ever", 1)
     */
    val wordcount = myresults.filter{case (_, count1) => count1 >= 2} //Gets words appearing more than 2 times
    /* RDD wordcount:
     * ("the", 2)
     */
    wordcount
  }
  
  def expectedOutput(sc: SparkContext): RDD[(String, Int)] = {
    // Example expected output RDD for testing
    val expectedList = List(
      ("the", 2)
    )
    sc.parallelize(expectedList) //Transformation, no computation
  }
  
  def saveit(name: String, myresults: org.apache.spark.rdd.RDD[(String, Int)]) = {
    /* 
     * Saves an RDD in HDFS in a directory specified by the "name" variable.
     */ 
    myresults.saveAsTextFile(name)
  }
}
