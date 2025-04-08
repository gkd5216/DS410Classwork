// Mandatory imports for Spark RDDs
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Q1 {
  /* The purpose of this program is to use the War and Peace dataset in /datasets/wap
   * to compute the variation of wordcount including the count of words that have a 
   * lowercase "e"
   * A word is any sequence of characters that has no space in it
   */

  def main(args: Array[String]): Unit = {
    val sc = getSC() //Initializes SparkContext
    val myRDD = getRDD(sc) //Loads dataset
    val myresults = doWordCount(myRDD) //Processes data to get word count
    saveit("spark1output", myresults) //Saves results to HDFS
  }

  def getSC() = {
    /* Gets the spark context variable with
     * application name "Q1 Word Count"
     */
    val conf = new SparkConf().setAppName("Q1 Word Count") //Change app name
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

  def getTestRDD(sc: SparkContext): RDD[String] = { //Creates a small testing RDD
    val testlist = List( //Retailtab sample data
      "the quick brown fox",
      "jumps over the lazy dog"
    )
    sc.parallelize(testlist, 2)
  }

  def doWordCount(input: RDD[String]): RDD[(String, Int)] = {
    /* The purpose of this queston is to count the number of times that
     * each word appears in the input RDD
     * 
     * Input: an RDD where each entry is a string, corresponding to a line of text
     * Output: an RDD where each entry is a (word, count) tuple. 
     * 
     * This function works by reading the RDD, splitting the lines by whitespace characters,
     * removing blank words, then converting each occurence of a word into a tuple (word, 1) so that we 
     * can add up all of the 1's in the occurrences associated with a word
     */
    val words = input.flatMap(line => line.split("\\W+")) //Split lines by whitespace
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
     */
    val noblank = words.filter(line => line.size > 0)
    /* RDD noblank:
     * "the"
     * "quick"
     * "brown"
     * "fox"
     * "jumps"
     * "over"
     * "lazy"
     * "dog"
     */
    val ewords = noblank.filter{word => word.contains("e")} // Filter for non empty words
    /* RDD ewords:
     * "the"
     * "over"
     */
    val kv = ewords.map(word => (word, 1)) // Maps each word to (word, 1)
    /* RDD kv:
     * ("the", 1)
     * ("over", 1)
     */
    val myresults = kv.reduceByKey((count1, count2) => count1 + count2) //Sum results for each word
    /* RDD wordsum:
     * ("the", 2)
     * ("over", 1)
     */
    myresults
  }

  def expectedOutput(sc: SparkContext): RDD[(String, Int)] = {
    // Example expected output RDD for testing
    val expectedlist = List(
      ("the", 2),
      ("over", 1)
    )
    sc.parallelize(expectedlist)
  }

  def saveit(name: String, myresults: org.apache.spark.rdd.RDD[(String, Int)]) = {
    /* 
     * Saves an RDD in HDFS in a directory specified by the "name" variable.
     */ 
    myresults.saveAsTextFile(name)
  }
}
