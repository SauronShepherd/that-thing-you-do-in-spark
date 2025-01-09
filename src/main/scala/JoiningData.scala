import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.expr
import Utils._

object JoiningData {

  /**
   * Performs various join operations on the provided DataFrames and returns a final DataFrame.
   *
   * - Joins two copies of the data on a date condition.
   * - Performs multiple broadcast joins with coalesced DataFrame.
   *
   * @param df The original DataFrame to perform the join operations on.
   * @param filteredCoaDF The coalesced DataFrame used for concatenating multiple joins.
   * @return A DataFrame resulting from the concatenation of the join operations.
   */
  def run(df: DataFrame, filteredCoaDF: DataFrame): DataFrame = {

    // Define the join condition: start date must be less than end date
    val onCondition = expr("start.date < end.date")

    // Alias the df for the start and end DataFrames
    val startDF = df.alias("start")
    val endDF = df.alias("end")

    // Print the number of partitions after performing an inner join
    printNumParts("Inner join", startDF.join(endDF, onCondition))

    // Define the range of indexes for the multiple broadcast joins
    val indexes = 2 to 3

    // Prepare the first DataFrame to join by coalescing it into a single partition and renaming the 'month' column
    val firstDF = filteredCoaDF.coalesce(1).withColumnRenamed("month", "month1").alias("df1")

    // Coalesce the original DataFrame into one partition to prepare it for joining
    val onePartitionDF = df.coalesce(1)

    // Perform a series of joins using the foldLeft method to concatenate joins on the 'month' column
    val multiJoinDF = indexes.foldLeft(firstDF) { (currentDF, index) =>
      val prevIndex = index - 1

      // Perform the join between the current DataFrame and onePartitionDF on the 'month' column
      val nextDF = printNumParts(
        "Multiple broadcast joins",
        currentDF.join(onePartitionDF,
                       currentDF(s"month$prevIndex") <= df("month"))
                 .select(currentDF("*"), df("month").as(s"month$index"))
      )

      // Return the DataFrame after the join
      nextDF
    }

    // Return the final DataFrame after performing all joins
    multiJoinDF
  }

}
