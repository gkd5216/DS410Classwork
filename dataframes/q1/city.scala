import org.apache.spark.sql.{Dataset, DataFrame, SparkSession, Row}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.udf

object Q1 {
  /* The purpose of this program is to use the Cities dataset in /datasets/cities to
   * compute, for every state abbreviation (in the dataset, Washington D.C. counts as 
   * a state), the number of cities it has, the total population in the state, and the 
   * maximum number of zip codes that appear in a city (in the state).
   */
  
  def main(args: Array[String]) = {  // this is the entry point to our code
    // do not change this function
    val spark = getSparkSession()
    import spark.implicits._
    val mydf = getDF(spark) 
    val counts = doCity(mydf) 
    saveit(counts, "dataframes_q1")  // save the rdd to your home directory in HDFS
  }
  
  def registerZipCounter(spark: SparkSession) = {
    val zipCounter = udf({x: String => Option(x) match {case Some(y) => y.split(" ").size; case None => 0}})
    spark.udf.register("zipCounter", zipCounter) // registers udf with the spark session
  }
  
  def doCity(input: DataFrame): DataFrame = {
    /* The purpose of this function is to compute, for every state 
     * abbreviation (in the dataset, Washington D.C. counts as a state), 
     * the number of cities it has, the total population in the state, 
     * and the maximum number of zip codes that appear in a city
     */

    val zipcodes = input.withColumn("ZipCodeCount", expr("zipCounter(ZipCodes)"))
    val grouped = zipcodes.groupBy("StateAbbreviation")
      .agg(
        count("City").as("NumberofCities"),
        sum("Population").as("TotalPopulation"),
        max("ZipCodeCount").as("MaximumNumberofZipCodes"))

    grouped
  }
  
  def getDF(spark: SparkSession): DataFrame = {
    // when spark reads dataframes from csv, when it encounters errors it just creates lines with nulls in 
    // some of the fields, so you will have to check the slides and the df to see where the nulls are and
    // what to do about them
    
    val mySchema = (new StructType() //Reads the dataset by adding column names, types, and T/F for missing values
      .add("City", StringType, true)
      .add("StateAbbreviation", StringType, true)
      .add("State", StringType, true)
      .add("County", StringType, true)
      .add("Population", IntegerType, true)
      .add("ZipCodes", StringType, true)
      .add("ID", StringType))

    val mydf = spark.read
      .format("csv") //Explores cities.csv
      .option("header", "true")
      .option("delimiter", "\t")
      .schema(mySchema) //Reads the datatypes, column names and T/F for missing values
      .load("/datasets/cities/cities.csv") //Dataset location
      .na.drop("any", Seq("City", "StateAbbreviation", "Population")) //Drops null values

    mydf
  }
  
  def getSparkSession(): SparkSession = {
    val spark = SparkSession.builder().getOrCreate()
    registerZipCounter(spark) // tells the spark session about the UDF
    spark
  }
  
  def getTestDF(spark: SparkSession): DataFrame = {
    import spark.implicits._ //so you can use .toDF
    // check slides carefully. note that you don't need to add headers, unlike RDDs
    Seq(
      ("San Bernardino", "CA", "California", "San Bernardino", "221041", "92404 92413 92418 92427", "1840021728"),
      ("Los Angeles", "CA", "California", "Los Angeles", "220929", "32608 32614 32627 32635", "1840014022"),
      ("Spring Valley", "NV", "Nevada", "Clark", "220114", "89147 89117 89164 89173", "1840033832"),
      ("Virginia Beach", "VA", "Virginia", "Virginia Beach", "219234", "98422 98406 98481 98490", "1840021129"),
      ("Roanoke", "VA", "Virginia", "Roanoke", "218533", "24019 24037 24038 24043", "1840003858")
    ).toDF("City", "StateAbbreviation", "State", "County", "Population", "ZipCodes", "ID") //Creates a dataframe with cities dataset columns
  }
  
  def expectedOutput(spark: SparkSession): DataFrame = {
    import spark.implicits._ //so you can use .toDF
    Seq(
      ("CA", 2, 441970, 4),
      ("NV", 1, 220114, 4),
      ("VA", 2, 437767, 4)
    ).toDF("StateAbbreviation", "NumberofCities", "TotalPopulation", "MaximumNumberofZipCodes") 
    //Includes a dataframe with the number of cities, total population, and max number of zip codes for every state abbreviation
  }
  
  def saveit(counts: DataFrame, name: String) = {
    counts.write.format("csv").mode("overwrite").save(name)
  }

}
