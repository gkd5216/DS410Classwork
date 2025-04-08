// Mandatory imports for Spark RDDs
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Q3 {
  /* The purpose of this program is to use the retailtab dataset in /datasets/wap
   * to compute the total cost of order for each order ID (InvoiceNo).
   * Each line is a record representing a customer order item
   * The InvoiceNo field is defined as the order ID.
   * All lines belonging to the same order have the same InvoiceNo
   */

  def main(args: Array[String]): Unit = {
    val sc = getSC() //Initializes SparkContext
    val myRDD = getRDD(sc) //Loads dataset
    val myresults = doRetail(myRDD) //Processes data to get total cost
    saveit("spark3output", myresults) //Saves results to HDFS 
  }
  
  def getSC(): SparkContext = { 
    /* Gets the spark context variable with
     * application name "Q3 total cost"
     */
    val conf = new SparkConf().setAppName("Q3 total cost")
    val sc = new SparkContext(conf)
    sc
  }

  def getRDD(sc: SparkContext): RDD[String] = { //Document the RDD
    /*
     * This is an RDD in which each entry is a string corresponding
     * to a line of text.
     */
    sc.textFile("/datasets/retailtab")
  }

  def getTestRDD(sc: SparkContext): RDD[String] = { //Creates a small testing RDD
    val testlist = List( //Retailtab sample data
      "InvoiceNo\tStockCode\tDescription\tQuantity\tInvoiceDate\tUnitPrice\tCustomerID\tCountry", // Header
      "536365\t85123A\tWHITE T-LIGHT HOLDER\t6\t12/1/2010 8:26\t2.55\t17850\tUnited Kingdom",
      "536365\t71053\tWHITE METAL LANTERN\t6\t12/1/2010 8:26\t3.39\t17850\tUnited Kingdom",
      "536366\t84406B\tCREAM CUPID HEARTS\t8\t12/1/2010 8:26\t2.75\t17850\tItaly"
    )
    sc.parallelize(testlist)
  }
  
  def doRetail(input: RDD[String]): RDD[(String, Double)] = {
    /* The purpose of this question is to count the total cost of order for each
     * order ID (InvoiceNo)
     * 
     * Input: an RDD in which each entry is a string, corresponding to a line of text
     * Output: an RDD where each entry is a (orderid, count) tuple. The orderid is a string
     * and the count is a double and contains the total cost computed by the sum of
     * quantity * unitprice
     *
     * This function works by reading the RDD, checking if a header row exists,
     * splitting each line by tab, maps quantity and unitprice for each orderid, 
     * and computes the total cost for each order
     */ 
    val noheader = input.filter(line => !line.contains("InvoiceNo")) //Skip header

    /* RDD noheader:
     * 536365\t85123A\tWHITE T-LIGHT HOLDER\t6\t12/1/2010 8:26\t2.55\t17850\tUnited Kingdom",
      "536365\t71053\tWHITE METAL LANTERN\t6\t12/1/2010 8:26\t3.39\t17850\tUnited Kingdom",
      "536366\t84406B\tCREAM CUPID HEARTS\t8\t12/1/2010 8:26\t2.75\t17850\tItaly"
      */
    val invoiceNoIndex = 0 //InvoiceNo (orderID) column
    val quantityIndex = 3 //Quantity column
    val unitPriceIndex = 5 //UnitPrice column
    noheader
      .map{line =>
        val splitcolumns = line.split("\t") //Split by tab
        /* RDD splitcolumns:
         * "536365",
         * "85123A",
         * "WHITE T-LIGHT HOLDER",
         * "6",
         * "12/1/2010 8:26",
         * "2.55",
         * "17850",
         * "United Kingdom",
         * "536365",
         * "71053",
         * "WHITE METAL LANTERN",
         * "6",
         * "12/1/2010 8:26",
         * "3.39",
         * "17850",
         * "United Kingdom",
         * "536366",
         * "84406B",
         * "CREAM CUPID HEARTS",
         * 8
         * "12/1/2010 8:26",
         * "2.75",
         * "17850",
         * "Italy"
         */
        val invoiceNo = splitcolumns(invoiceNoIndex) //Get invoiceNo
        /* RDD invoiceNo:
         * "536365",
         * "536365",
         * "536366"
         */
        val quantity = splitcolumns(quantityIndex).toDouble //Get quantity and convert to double
        /* RDD quantity:
         * "6.0",
         * "6.0",
         * "8.0"
         */
        val unitPrice = splitcolumns(unitPriceIndex).toDouble //Get unitprice and convert to double
        /* RDD unitPrice:
         * "2.55",
         * "3.39",
         * "2.75"
         */
        (invoiceNo, quantity * unitPrice) //Gets orderid and total cost
        /* RDD output:
         * ("536365", 35.64),
         * ("536366", 22.00)
         */
      }
      .reduceByKey((count1, count2) => count1 + count2) //Aggregates all values with same key
       /* RDD reduceByKey:
       * ("536365", 35.64)
       * ("536366", 22.0)
       */
  }
  
  def expectedOutput(sc: SparkContext): RDD[(String, Double)] = { //Provides expected output for the test dataset
    val expectedlist = List(
      ("536365", 35.64),
      ("536366", 22.0)
    )
    sc.parallelize(expectedlist)
  }

  def saveit(name: String, myresults: org.apache.spark.rdd.RDD[(String, Double)]) = { 
    /*
     * Saves an RDD in HDFS in a directory specified by the "name" variable.
     */
    myresults.saveAsTextFile(name)
  }
}
