import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

object Q2 {  

    def main(args: Array[String]) = {  // autograder will call this function
        //remember, RDDs only
        val sc = getSC()  // one function to get the sc variable
        val myrdd = getRDD(sc) // on function to get the rdd
        val counts = doCity(myrdd) // additional functions to do the computation
        saveit(counts, "everything_q2")  // save the rdd to your home directory in HDFS
    }

    def getSC(): SparkContext = {
      val conf = new SparkConf().setAppName("Cities count")
      val sc = SparkContext.getOrCreate(conf)
      sc
    }

    def getRDD(sc:SparkContext): RDD[String] = { // get the big data rdd
      sc.textFile("/datasets/cities")
    }

    def doCity(input: RDD[String]): RDD[(Int, Int)] = {
      /* The purpose of this question is to compute the number of zipcodes as the key
       * and the number of cities as the value. 
       *
       * Input: an RDD in which each entry is a string, corresponding to a line of text.
       * Output: an RDD in which each entry is a tuple of integers.
       * The number of zipcodes is an integer, and the number of cities is the sum of cities.
       *
       * This function works by reading the RDD, checking if a header row exists, splitting each
       * line by tab, filtering for 6 or more columns, splitting by space for zipcodes, getting
       * the length of the zipcodes, and mapping cities for the number of zipcodes.
       */

      //val noheader = input.filter(line => !line.startsWith("City")) //Skip header
        //.split("\t").length >= 6
      //val zipcodeIndex = 5 //Zipcode index
      /*
      noheader
        .map{line =>
          val splitcolumns = line.split("\t") //Split by tab
          //if (splitcolumns.length >= 6) {
          val zipcode = splitcolumns(zipcodeIndex).split(" ").length
          (zipcode, 1)
        } 
        .reduceByKey((key, value) => key + value)
        
          .aggregateByKey(0)(
          (key, value) => key + value,
          (key1, key2) => key1 + key2
        )
      */
      input
        .filter(line => !line.startsWith("City")) //Skip header
        .map(line => line.split("\t")) //Split by tab         
        .filter(column => column.length >= 6) //Cities dataset is 6 columns
        .map(column => {
          val zipColumn = column(5)
          val zipCount = if (zipColumn.isEmpty) 0 else zipColumn.split(" ").length
          (zipCount, 1)   
        })
        .reduceByKey(_ + _) 
    }
   
    def getTestRDD(sc: SparkContext): RDD[String] = { //Creates a small testing RDD
      val testlist = List( //Cities sample data 
        "City\tState Abbreviation\tState\tCounty\tPopulation\tZip Codes (space separated)\tID",
        "New York\tNY\tNew York\tQueens\t18908608\t11229 11222 11221 11220\t1840034016",
        "Los Angeles\tCA\tCalifornia\tLos Angeles\t11922389\t91367 90291 90293\t91611840020491",
        "Chicago\tIL\tIllinois\tCook\t8497759\t60018\t1840000494",
        "Miami\tFL\tFlorida\tMiami-Dade\t6080145\t33148 33149 33144\t1840015149",
        "Dallas\tTX\tTexas\tDallas\t5830932\t75235 75254 75251\t1840019440",
        "Boston\tMA\tMassachusetts\tSuffolk\t4328315\t02120 02121 02122\t1840000455"
      )
      sc.parallelize(testlist)
    }

    def expectedOutput(sc: SparkContext): RDD[(Int, Int)] = { //Provides expected output for the test dataset
      val expectedlist = List(
        (1, 1),
        (3, 4),
        (4, 1)
      )
      sc.parallelize(expectedlist)
    }
    

    def saveit(myrdd: RDD[(Int, Int)], name: String) = {
        myrdd.saveAsTextFile(name)
    }

}

