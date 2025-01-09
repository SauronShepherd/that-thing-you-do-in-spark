import org.apache.spark.sql.DataFrame
import Utils._

object FilteringData {

  /**
   * Filters the DataFrame based on the "January" month and prints the number of partitions
   * after applying different partitioning strategies (no change, repartition, coalesce).
   *
   * @param df The original DataFrame to filter.
   * @return A tuple containing:
   *         - A DataFrame filtered without changing partitioning.
   *         - A DataFrame filtered after repartitioning by "month".
   *         - A DataFrame filtered after coalescing to a single partition.
   */
  def run(df: DataFrame): (DataFrame, DataFrame, DataFrame) = {

    // Define a filter condition to select rows where the month is "January"
    val monthFilter = df("month") === "January"

    // Apply filtering without changing partitioning and print the number of partitions
    val janDF = printNumParts(
      "Filter without changing partitioning",
      df.where(monthFilter)
    )

    // Apply repartitioning by the "month" column and then filter, printing the number of partitions
    val janRepDF = printNumParts(
      "Repartition and then filter",
      df.repartition(df("month")).where(monthFilter)
    )

    // Apply coalescing to a single partition and then filter, printing the number of partitions
    val janCoaDF = printNumParts(
      "Coalesce and then filter",
      df.coalesce(1).where(monthFilter)
    )

    // Return the filtered DataFrames as a tuple
    (janDF, janRepDF, janCoaDF)
  }

}
