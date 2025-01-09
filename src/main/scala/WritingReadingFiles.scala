import org.apache.spark.sql.DataFrame
import java.nio.file.{Files, Paths}
import Utils._

object WritingReadingFiles {

  /**
   * Performs a series of operations on the given DataFrame:
   * - Repeatedly creates a union of the DataFrame with itself and writes it to a specified folder path.
   * - Loads the files back into a DataFrame and prints the number of partitions.
   * - Counts and prints the number of Parquet files in the folder.
   *
   * @param df The DataFrame to be processed.
   * @return The DataFrame loaded from the files after all operations.
   */
  def run(df: DataFrame): DataFrame = {

    // Get the current Spark session
    var spark = df.sparkSession

    var readFilesDF = df

    // Loop to perform the operations 5 times
    for (i <- 1 to 5) {
      // Define the folder path where the DataFrame will be written
      val folderPath = s"data/df$i"

      // Perform union with itself and write to the specified folder, overwriting if exists
      readFilesDF.union(df)
        .write
        .mode("overwrite")
        .save(folderPath)

      // Load the written files back into a DataFrame and print the number of partitions
      readFilesDF = printNumParts("Read data from files", spark.read.load(folderPath))

      // Count and print the number of Parquet files in the folder
      val parquetFilesCount = Files.list(Paths.get(folderPath))
        .filter(path => path.toString.endsWith(".parquet")) // Filter for Parquet files
        .count() // Count the number of Parquet files

      // Print the number of Parquet files found in the folder
      println(s"Parquet files count: $parquetFilesCount\n")
    }

    // Set a configuration parameter for maximum partition size
    spark.conf.set("spark.sql.files.maxPartitionBytes", "18432") // 18 KB max partition size

    // Load the data with the updated configuration and print the number of partitions
    printNumParts(
      "Read data from files with 18 KB maxPartitionBytes",
      spark.read.load("data/df5")
    )

    // Stop the current Spark session
    spark.stop()

    // Create a new Spark session with custom configuration for maxPartitionBytes
    //spark = createSparkSession(Map("spark.sql.files.maxPartitionBytes" -> "18432"))
    spark = createSparkSession()
    spark.conf.set("spark.sql.files.maxPartitionBytes", "18432") // 18 KB max partition size

    // Read the data again with the new session and print the number of partitions
    readFilesDF = printNumParts(
      "Read data from files with 18 KB maxPartitionBytes (new Spark session)",
      spark.read.load("data/df5")
    )

    // Return the final DataFrame
    readFilesDF
  }

}
