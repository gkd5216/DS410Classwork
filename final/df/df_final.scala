import org.apache.spark.sql.{Dataset, DataFrame, SparkSession, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._


object DFFinal {
    /* The purpose of this program is to use the facebook dataset in 
     * /datasets/facebook to compute
     *
     */


    def main(args: Array[String]) = {  // autograder will call this function
        //remember, DataFrames only
        val spark = getSparkSession()
        import spark.implicits._
        val mydf = getDF(spark)
        val answer = doFinal(mydf)
        saveit(answer, "df_final")

    }

    def doFinal(input: DataFrame): DataFrame = {
      /* The purpose of this function is to compute
       *
       */

      //import spark.implicits._

      //val node = mydf.getString(0)
      //val nodeCount = mydf.getInt(2)
      val grouped = input.groupBy(mydf.node)
        .agg(sum(mydf.nodeCount).as("LeftCount"))
        .agg(sum(mydf.nodeCount).as("RightCount"))
        .where("LeftCount" >= 3 and "RightCount" >= 3)
      
      grouped
    }

    def getSparkSession(): SparkSession = {
        val spark = SparkSession.builder().getOrCreate()
        spark
    }

    def getDF(spark: SparkSession): DataFrame = {
      val mydf = spark.read
          .format("csv")
          .option("header", "false")
          .option("delimiter", " ")
          .load(
            "/datasets/facebook/0.edges",
            "/datasets/facebook/107.edges",
            "/datasets/facebook/1684.edges",
            "/datasets/facebook/1912.edges",
            "/datasets/facebook/3437.edges",
            "/datasets/facebook/348.edges",
            "/datasets/facebook/3980.edges",
            "/datasets/facebook/414.edges",
            "/datasets/facebook/686.edges",
            "/datasets/facebook/698.edges"
          )
          .na.drop("any") //Drop null values
        val node = mydf.getString(0)
        val nodeCount = mydf.getInt(2)

      node; nodeCount
    }
    
    def getTestDF(spark: SparkSession): DataFrame = {
        import spark.implicits._

        Seq(
          ("234", "234"),
          ("234", "234"),
          ("234", "234"),
          ("234", "234"),
          ("993", "234"),
          ("993", "1124"),
          ("993", "1134")
        ).toDF("node1", "node2")
    }

    def expectedOutput(spark: SparkSession): DataFrame = {
        import spark.implicits._

        Seq(
          ("234", (4, 5)),
          ("993", (3, 0))
        ).toDF("node", "leftcount", "rightcount")

    }
 
    def saveit(counts: DataFrame, name: String) = {
      counts.write.format("csv").mode("overwrite").save(name)

    }

}


