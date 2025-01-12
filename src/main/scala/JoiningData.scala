import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import scala.collection.mutable.ListBuffer
import Utils._


object JoiningData {

  /**
   * Performs various join operations on the provided DataFrames and returns a final DataFrame.
   *
   * - Joins two copies of the data on a date condition.
   * - Performs multiple broadcast joins with coalesced DataFrame.
   *
   * @param df         The original DataFrame containing all data.
   * @param filteredDF     The DataFrame filtered for specific criteria.
   * @param filteredRepDF  The repartitioned and filtered DataFrame.
   * @param filteredCoaDF  The coalesced and filtered DataFrame.
   * @return A DataFrame resulting from the concatenation of the join operations.
   */
  def run(df: DataFrame, filteredDF: DataFrame,
          filteredRepDF: DataFrame, filteredCoaDF: DataFrame): DataFrame = {

    /**
     * Perform joins between all combinations of input DataFrames based on a specific join condition.
     *
     * @param threshold The current value of spark.sql.autoBroadcastJoinThreshold.
     * @param dfs A map of DataFrames to join, with descriptive keys.
     * @return A list of DataFrames resulting from the join operations.
     */
    def performJoins(threshold: String, dfs: Map[String, DataFrame]): List[DataFrame] = {

      // Define the join condition: start date must be less than end date
      val onCondition = expr("start.date < end.date")

      // Generate all combinations of the input DataFrames for pairwise joins
      val combinations = for {
        (startKey, startDF) <- dfs
        (endKey, endDF) <- dfs
      } yield (startKey, startDF, endKey, endDF)

      val resultDFs = new ListBuffer[DataFrame]()

      // Perform joins and log the time taken
      for ((startKey, startDF, endKey, endDF) <- combinations) {
        val text = s"Joining start_df($startKey) with end_df($endKey) with autoBroadcastJoinThreshold=$threshold"

        // Perform the join operation with aliases for clarity
        val resultDF = startDF.alias("start").join(endDF.alias("end"), onCondition)

        // Measure and log the time taken for the join, and append the result to the list
        resultDFs.append(printTime("", printNumParts(text, resultDF)))
      }

      resultDFs.toList
    }

    // Define input DataFrames with descriptive keys
    val inputDFs = Map(
      "all data" -> df,
      "filtered" -> filteredDF,
      "filtered + repartitioned" -> filteredRepDF,
      "filtered + coalesced" -> filteredCoaDF
    )

    // Retrieve the Spark session from one of the DataFrames
    val spark = df.sparkSession

    // Get the current value of the autoBroadcastJoinThreshold
    val autoBroadcastJoinThreshold = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")

    // Perform joins using the current autoBroadcastJoinThreshold
    val joinDFs = performJoins(autoBroadcastJoinThreshold, inputDFs)

    // Unpersist all intermediate join results to free up memory
    for(joinDF <- joinDFs)
      joinDF.unpersist()

    // Temporarily set the autoBroadcastJoinThreshold to "-1" (disable autobroadcast)
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

    // Perform joins with the broadcast threshold disabled
    performJoins("-1", inputDFs)

    // Restore the original autoBroadcastJoinThreshold value
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", autoBroadcastJoinThreshold)

    // Prepare the first DataFrame to join by coalescing it into a single partition and renaming the 'month' column
    val firstDF = filteredCoaDF.withColumnRenamed("month", "month1").alias("df1")

    // Coalesce the original DataFrame into one partition to prepare it for joining
    val onePartitionDF = df.coalesce(1)

    // Perform a series of chained joins using a loop to concatenate results based on the 'month' column
    var multiJoinDF = firstDF
    for (index <- 2 to 3) {
      val prevIndex = index - 1

      // Log and perform the join operation between the current result and onePartitionDf
      multiJoinDF = printNumParts(
        "Multiple broadcast joins",
        multiJoinDF.alias("start").join(
          onePartitionDF.alias("end"),
          expr(s"start.month$prevIndex <= end.month") // Join condition on sequential month columns
        ).select(multiJoinDF("*"), onePartitionDF("month").alias(s"month$index"))
      )
    }

    // Return the final DataFrame after performing all joins
    multiJoinDF
  }

}
