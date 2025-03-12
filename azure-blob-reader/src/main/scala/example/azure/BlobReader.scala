package example.azure

import org.apache.spark.sql.SparkSession
import com.azure.identity.DefaultAzureCredentialBuilder
import com.azure.storage.blob.BlobServiceClientBuilder
import com.azure.storage.blob.models.UserDelegationKey
import com.azure.storage.common.sas.SasProtocol
import com.azure.storage.blob.sas.BlobSasPermission
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues

import java.time.OffsetDateTime
import java.time.temporal.ChronoUnit

object BlobReader extends App {

  def generateUserDelegationSas(
      accountName: String,
      containerName: String,
      blobName: String
  ): String = {
    val credential = new DefaultAzureCredentialBuilder().build()

    // Build the Blob Service Client
    val blobServiceClient = new BlobServiceClientBuilder()
      .credential(credential)
      .endpoint(s"https://$accountName.blob.core.windows.net")
      .buildClient()

    // Get User Delegation Key (valid for 1 hour)
    val expiryTime = OffsetDateTime.now().plusHours(1)
    val delegationKey: UserDelegationKey =
      blobServiceClient.getUserDelegationKey(OffsetDateTime.now(), expiryTime)

    // Set SAS permissions (read-only)
    val permissions = new BlobSasPermission().setReadPermission(true)

    // Generate SAS token using correct signature values
    val sasValues = new BlobServiceSasSignatureValues(expiryTime, permissions)
      .setProtocol(SasProtocol.HTTPS_ONLY)

    val blobClient = blobServiceClient
      .getBlobContainerClient(containerName)
      .getBlobClient(blobName)

    val sasToken = blobClient.generateUserDelegationSas(
      sasValues,
      delegationKey
    )

    sasToken
  }

  // Initialize Spark Session
  val spark = SparkSession
    .builder()
    .appName("SparkAzureSasExample")
    .config("spark.master", "local[*]") // Use multiple cores for performance
    .getOrCreate()

  val accountName = "sparkblobpoc"
  val containerName = "pricing"
  val blobName = "ComputePricing.json"

  // Generate SAS Token
  val sasToken = generateUserDelegationSas(accountName, containerName, blobName)

  val blobUrl =
    "wasbs://pricing@sparkblobpoc.blob.core.windows.net/ComputePricing.json"

  println(s"Blob URL: $blobUrl")

  spark.conf.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
  spark.conf.set(
    "fs.azure.sas.pricing.sparkblobpoc.blob.core.windows.net",
    sasToken
  )

  // Read JSON file from Azure Blob Storage
  try {
    val df = spark.read.option("multiline", "true").json(blobUrl)

    // Show DataFrame content
    df.show()

  } catch {
    case e: Exception =>
      println(s"Error reading JSON file from Blob Storage: ${e.getMessage}")
      e.printStackTrace()
  }

  // Stop Spark Session
  spark.stop()
}
