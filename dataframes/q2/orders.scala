//add the imports
import org.apache.spark.sql.{Dataset, DataFrame, SparkSession, Row}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.udf

object Q2 {

    def main(args: Array[String]) = {  // this is the entry point to our code
        // do not change this function
        val spark = getSparkSession()
        import spark.implicits._
        val (c, o, i) = getDF(spark)
        val counts = doOrders(c,o,i)
        saveit(counts, "dataframes_q2")  // save the rdd to your home directory in HDFS
    }


    def doOrders(customers: DataFrame, orders: DataFrame, items: DataFrame): DataFrame = {
      /* The purpose of this function is to compute, for every country, 
       * the amount of money spent from that country. 
       */

      val custid_join_condition = customers.col("CustomerID") === orders.col("CustomerID")
      val custord_joined_df = customers.join(orders, custid_join_condition)
      val stockcode_join_condition = orders.col("StockCode") === items.col("StockCode")
      val joined_df = custord_joined_df.join(items, stockcode_join_condition)
      val moneySpent = joined_df.withColumn("AmountofMoney", col("UnitPrice") * col("Quantity")) 
      val grouped = moneySpent.groupBy("Country")
        .agg(
          sum("AmountofMoney").as("MoneySpent"))

      grouped

    }

    def getDF(spark: SparkSession): (DataFrame, DataFrame, DataFrame) = {
      val customerSchema = (new StructType()
        .add("CustomerID", StringType, true)
        .add("Country", StringType, true))

      val itemsSchema = (new StructType()
        .add("StockCode", StringType, true)
        .add("Description", StringType, true)
        .add("UnitPrice", DoubleType, true))

      val ordersSchema = (new StructType()
        .add("InvoiceNo", StringType, true)
        .add("StockCode", StringType, true)
        .add("Quantity", IntegerType, true)
        .add("InvoiceDate", StringType, true)
        .add("CustomerID", StringType, true))

    val customerdf = spark.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", "\t")
      .schema(customerSchema)
      .load("/datasets/orders/customers.csv")
      .na.fill("blank", Seq("CustomerID")) 
      //.na.drop("any", Seq("Country")) //Drops null values

    val itemsdf = spark.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", "\t")
      .schema(itemsSchema)
      .load("/datasets/orders/items.csv")
      .na.drop("any", Seq("UnitPrice")) //Drops null values

    val ordersdf = spark.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", "\t")
      .schema(ordersSchema)
      .load(
        "/datasets/orders/orders-00000",
        "/datasets/orders/orders-00001",
        "/datasets/orders/orders-00002",
        "/datasets/orders/orders-00003",
        "/datasets/orders/orders-00004",
        "/datasets/orders/orders-00005",
        "/datasets/orders/orders-00006",
        "/datasets/orders/orders-00007"
      )
      .na.fill("blank", Seq("CustomerID"))
      .na.drop("any", Seq("Quantity")) //Drops null values

    (customerdf, ordersdf, itemsdf)
    }

    def getSparkSession(): SparkSession = {
        val spark = SparkSession.builder().getOrCreate()
        spark
    }

    def getTestDF(spark: SparkSession): (DataFrame, DataFrame, DataFrame) = {
        // don't forget the spark.implicits
        import spark.implicits._
        // return 3 dfs (customers, orders, items)
        val customers = Seq(
          (12346, "United Kingdom"),
          (12347, "Iceland")
        ).toDF("CustomerID", "Country")

        val orders = Seq(
          ("536365", "85123A", 6, "12/1/2010 8:26", 12346),
          ("536365", "85123A", 6, "12/1/2010 8:26", 12346),
          ("536366", "71053", 6, "12/1/2010 8:34", 12347)
        ).toDF("InvoiceNo", "StockCode", "Quantity", "InvoiceDate", "CustomerID")

        val items = Seq(
          ("85123A", "PINK FLOCK GLASS CANDLEHOLDER", 1.65),
          ("85123A", "CHILLI LIGHTS", 9.96),
          ("71053", "ART LIGHTS,FUNK MONKEY", 5.91)
        ).toDF("StockCode", "Description", "UnitPrice")

        (customers, orders, items)
    }
    def expectedOutput(spark: SparkSession) = {
        import spark.implicits._

        Seq(
          ("United Kingdom", 9.90),
          ("Iceland", 35.46)
        ).toDF("Country", "MoneySpent")


    }

    def saveit(counts: DataFrame, name: String) = {
      counts.write.format("csv").mode("overwrite").save(name)

    }

}

