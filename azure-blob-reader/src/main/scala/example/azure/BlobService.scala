package example.azure

import com.azure.identity.DefaultAzureCredentialBuilder
import com.azure.storage.blob.BlobServiceClientBuilder
import com.azure.storage.blob.models.UserDelegationKey
import com.azure.storage.blob.sas.{
  BlobSasPermission,
  BlobServiceSasSignatureValues
}
import com.azure.storage.common.sas.SasProtocol

import java.time.OffsetDateTime

object BlobService {

  def getBlobServiceClient(accountName: String) = {
    val credential = new DefaultAzureCredentialBuilder().build()
    new BlobServiceClientBuilder()
      .credential(credential)
      .endpoint(s"https://$accountName.blob.core.windows.net")
      .buildClient()
  }

  def generateUserDelegationSas(
      accountName: String,
      containerName: String,
      blobName: String
  ): String = {
    val blobServiceClient = getBlobServiceClient(accountName)

    val expiryTime = OffsetDateTime.now().plusHours(1)
    val delegationKey: UserDelegationKey =
      blobServiceClient.getUserDelegationKey(OffsetDateTime.now(), expiryTime)

    val permissions = new BlobSasPermission().setReadPermission(true)
    val sasValues = new BlobServiceSasSignatureValues(expiryTime, permissions)
      .setProtocol(SasProtocol.HTTPS_ONLY)

    val blobClient = blobServiceClient
      .getBlobContainerClient(containerName)
      .getBlobClient(blobName)

    blobClient.generateUserDelegationSas(sasValues, delegationKey)
  }
}
