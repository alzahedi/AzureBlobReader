package example.azure

import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.{Failure, Success, Try}

object SparkUtils {

  def createSparkSession(): SparkSession = {
    SparkSession
      .builder()
      .appName("SparkAzureSasExample")
      .config("spark.master", "local[*]")
      .getOrCreate()
  }

  def readJsonFromBlob(
      spark: SparkSession,
      blobUrl: String,
      sasToken: String,
      accountName: String,
      containerName: String
  ): Unit = {
    spark.conf.set(
      "fs.azure",
      "org.apache.hadoop.fs.azure.NativeAzureFileSystem"
    )
    spark.conf.set(
      s"fs.azure.sas.$containerName.$accountName.blob.core.windows.net",
      sasToken
    )

    Try(spark.read.option("multiline", "true").json(blobUrl)) match {
      case Success(df) =>
        df.show()
      case Failure(exception) =>
        println(
          s"Error reading JSON file from Blob Storage: ${exception.getMessage}"
        )
        exception.printStackTrace()
    }
  }
}
