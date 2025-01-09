import org.apache.spark.sql.DataFrame
import Utils._

object GroupingData {

  /**
   * Processes and prints the number of partitions for grouped DataFrames.
   *
   * @param dataDF         The original DataFrame containing all data.
   * @param filteredDF     The DataFrame filtered for specific criteria.
   * @param filteredRepDF  The repartitioned and filtered DataFrame.
   * @param filteredCoaDF  The coalesced and filtered DataFrame.
   */
  def run(dataDF: DataFrame, filteredDF: DataFrame,
          filteredRepDF: DataFrame, filteredCoaDF: DataFrame): Unit = {

    printNumParts("Count grouped by month (all data)",
                  dataDF.groupBy("month").count)
    printNumParts("Count grouped by month (filtered data)",
                  filteredDF.groupBy("month").count)
    printNumParts("Count grouped by month (filtered data repartitioned)",
                  filteredRepDF.groupBy("month").count)
    printNumParts("Count grouped by month (filtered data coalesced)",
                  filteredCoaDF.groupBy("month").count)

  }

}
