import org.apache.spark.sql.{Dataset, DataFrame, SparkSession, Row}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.udf

object Q3 {
  /* The purpose of this program is to use the Cities dataset in /datasets/cities to
   * compute the number of zip codes and the number of cities. 
   */

    def main(args: Array[String]) = {  // autograder will call this function
        //remember, DataFrames only
        val spark = getSparkSession()
        import spark.implicits._
        val mydf = getDF(spark)
        val answer = doCity(mydf)
        saveit(answer, "everything_q3")

    }

    def registerZipCounter(spark: SparkSession) = {
        val zipCounter = udf({x: String => Option(x) match {case Some(y) => y.split(" ").size; case None => 0}})
        spark.udf.register("zipCounter", zipCounter) // registers udf with the spark session
    }

    def doCity(input: DataFrame): DataFrame = {
      /* The purpose of this function is to compute the number of zip codes and
       * the number of cities.
       *
       */
      val nonulls = input.withColumn(
        "ZipCodes",
        when(trim(col("ZipCodes")) === "", null).otherwise(col("ZipCodes"))
      )

      //val nocitynulls = nonulls.filter(col("City").isNotNull)
      val withZipCounts = nonulls.withColumn(
        "ZipCodeCount",
        expr("zipCounter(ZipCodes)")
      )

      val grouped = withZipCounts.groupBy("ZipCodeCount") //Group by zipcode count
        .agg(count("City").as("NumberOfCities")) //Gets number of cities
        .orderBy("ZipCodeCount") //To check 0 and 1 zipcodes
        
      grouped
    }

    def getSparkSession(): SparkSession = {
        // always use this to get the spark variable, even when using the shell
        val spark = SparkSession.builder().getOrCreate()
        registerZipCounter(spark) // tells the spark session about the UDF
        spark
    }

    def getDF(spark: SparkSession): DataFrame = {
        //no schema, no points

        val mySchema = (new StructType()
          .add("City", StringType, true)
          .add("StateAbbreviation", StringType, true)
          .add("State", StringType, true)
          .add("County", StringType, true)
          .add("Population", IntegerType, true)
          .add("ZipCodes", StringType, true)
          .add("ID", StringType))
        
        val mydf = spark.read
          .format("csv")
          .option("header", "true")
          .option("delimiter", "\t")
          .schema(mySchema) //Reads the datatypes, column names and T/F for missing values
          .load("/datasets/cities/cities.csv") //Dataset location
          .na.fill("", Seq("ZipCodes"))
          .na.drop("any", Seq("City")) //Drops null values
        
        mydf
    }
    
    def getTestDF(spark: SparkSession): DataFrame = {
        import spark.implicits._ // do not delete this

        Seq(
          ("New York", "NY", "New York", "Queens", "18908608", "11229 11222 11221", "1840034016"),
          ("Chicago", "IL", "Illinois", "Cook", "8497759", "", "1840000494"), // No zip codes
          ("Miami", "FL", "Florida", "Miami-Dade", "6080145", "33148", "1840015149"),
          ("Dallas", "TX", "Texas", "Dallas", "5830932", "75235 75254", "1840019440"),
          ("Boston", "MA", "Massachusetts", "Suffolk", "4328315", "", "1840000455")
        ).toDF("City", "StateAbbreviation", "State", "County", "Population", "ZipCodes", "ID")
    }

    def expectedOutput(spark: SparkSession): DataFrame = {
        //return expected output of your test DF
        import spark.implicits._ // do not delete this

        Seq(
          (0, 2),
          (1, 1),
          (2, 1),
          (3, 1)
        ).toDF("ZipCodeCount", "NumberOfCities")
    }
 
    def saveit(counts: DataFrame, name: String) = {
      counts.write.format("csv").mode("overwrite").save(name)

    }

}


