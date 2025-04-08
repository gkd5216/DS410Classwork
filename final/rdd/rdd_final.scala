// Mandatory imports for Spark RDDs
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object RDDFinal {  

    def main(args: Array[String]) = {  // autograder will call this function
        //remember, RDDs only
        val sc = getSC()  // one function to get the sc variable
        val myrdd = getRDD(sc) // on function to get the rdd
        val answer = doFinal(myrdd) // additional functions to do the computation
        saveit(answer, "rdd_final")  // save the rdd to your home directory in HDFS
    }

    def getSC(): SparkContext = {
      val conf = new SparkConf().setAppName("FB Final") //Change app name
      val sc = new SparkContext(conf)
      sc
    }

    def getRDD(sc:SparkContext): RDD[String] = { // get the big data rdd
      sc.textFile("/datasets/facebook")
    }

    def doFinal(input: RDD[String]): RDD[(String, (Int, Int))] = {
      /* The purpose of this question is to
       *
       * Input: an RDD where each entry is a
       * Output: an RDD where each entry is a
       *
       * This function works by
       *
       */
      val splitting = input.flatMap(line => line.split("\\s+"))
        //.flatMap(line => line.split(" "))
      val filtering = splitting.filter(line => line.size > 0)
      filtering
        .map(node => {
          //val nodeCount = if (node.isEmpty) 0 else node.split(" ").length
          (node, (1, 1))
        })
          .reduceByKey{case ((node1, count1), (node2, count2)) =>
            (node1 + node2, count1 + count2)
          }
            .filter{case (_, (node1, node2)) => node1 + node2 >= 3}
    }
   
    def getTestRDD(sc: SparkContext): RDD[String] = {
      val testlist = List(
        "234 234",
        "234 234",
        "234 234",
        "234 234",
        "993 234",
        "993 1124",
        "993 1134"
      )
      sc.parallelize(testlist)
    }

    def expectedOutput(sc: SparkContext): RDD[(String, (Int, Int))] = {
      val expectedlist = List(
        ("234", (4, 5)),
        ("993", (3, 0))
      )
      sc.parallelize(expectedlist)

    }

    def saveit(myrdd: RDD[(String, (Int, Int))], name: String) = {
        myrdd.saveAsTextFile(name)
    }

}

