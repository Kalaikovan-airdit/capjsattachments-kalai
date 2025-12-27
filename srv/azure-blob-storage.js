const { BlobServiceClient } = require('@azure/storage-blob')
const cds = require("@sap/cds")
const LOG = cds.log('attachments')
const utils = require('../lib/helper')
const { fetchfromDestination } = require('../lib/destinations')

module.exports = class AzureAttachmentsService extends require("./object-store") {

  /**
   * Creates or retrieves a cached Azure Blob Storage client for the given tenant
   * @returns {Promise<{blobServiceClient: import('@azure/storage-blob').BlobServiceClient, containerClient: import('@azure/storage-blob').ContainerClient}>}
   */
  async retrieveClient() {
    try {
      const { container_name, container_uri, sas_token } = await fetchfromDestination('aisp-attachments');

      const blobServiceClient = new BlobServiceClient(container_uri + "?" + sas_token)
      const containerClient = blobServiceClient.getContainerClient(container_name)

      const newAzureCredentials = {
        containerClient,
      }
      const tenant = cds.context.tenant || 'default';
      this.clientsCache.set(tenant, newAzureCredentials);

      LOG.debug('Azure Blob Storage client has been created successful', {

        containerName: containerClient.containerName
      })
      return newAzureCredentials;
    } catch (error) {
      LOG.error(
        'Failed to create tenant-specific Azure Blob Storage client', error,
        'Check Service Manager and Azure Blob Storage instance configuration')
      throw error
    }
  }

  /**
  * @inheritdoc
  */
  async put(attachments, data) {
    if (Array.isArray(data)) {
      LOG.debug('Processing bulk file upload', {
        fileCount: data.length,
        filenames: data.map(d => d.filename)
      })
      return Promise.all(
        data.map((d) => this.put(attachments, d))
      )
    }

    const startTime = Date.now()

    LOG.debug('Starting file upload to Azure Blob Storage', {
      attachmentEntity: attachments.name,
      tenant: cds.context.tenant
    })
    const { containerClient } = await this.retrieveClient()
    try {
      let { content: _content, ...metadata } = data
      const blobName = metadata.url

      if (!blobName) {
        LOG.error(
          'File key/URL is required for Azure Blob Storage upload', null,
          'Ensure attachment data includes a valid URL/key',
          { metadata: { ...metadata, content: !!_content } })
        throw new Error('File key is required for upload')
      }

      if (!_content) {
        LOG.error(
          'File content is required for Azure Blob Storage upload', null,
          'Ensure attachment data includes file content',
          { key: blobName, hasContent: !!_content })
        throw new Error('File content is required for upload')
      }

      const blobClient = containerClient.getBlockBlobClient(blobName)

      LOG.debug('Uploading file to Azure Blob Storage', {
        containerName: containerClient.containerName,
        blobName,
        filename: metadata.filename,
        contentSize: _content.length || _content.size || 'unknown'
      })

      // Handle different content types for update
      let contentLength
      const content = _content
      if (Buffer.isBuffer(content)) {
        contentLength = content.length
      } else if (content && typeof content.length === 'number') {
        contentLength = content.length
      } else if (content && typeof content.size === 'number') {
        contentLength = content.size
      } else {
        // Convert to buffer if needed
        const chunks = []
        for await (const chunk of content) {
          chunks.push(chunk)
        }
        _content = Buffer.concat(chunks)
        contentLength = _content.length
      }

      // The file upload has to be done first, so super.put can compute the hash and trigger malware scan
      await blobClient.upload(_content, contentLength)
      await super.put(attachments, metadata)

      const duration = Date.now() - startTime
      LOG.debug('File upload to Azure Blob Storage completed successfully', {
        filename: metadata.filename,
        fileId: metadata.ID,
        containerName: containerClient.containerName,
        blobName,
        duration
      })
    } catch (err) {
      const duration = Date.now() - startTime
      LOG.error(
        'File upload to Azure Blob Storage failed', err,
        'Check Azure Blob Storage connectivity, credentials, and container permissions',
        { filename: data?.filename, fileId: data?.ID, containerName: containerClient.containerName, blobName: data?.url, duration })
      throw err
    }
  }

  /**
  * @inheritdoc
  */
  async get(attachments, keys) {
    const startTime = Date.now()
    LOG.debug('Starting stream from Azure Blob Storage', {
      attachmentEntity: attachments.name,
      keys,
      tenant: cds.context.tenant
    })
    const { containerClient } = await this.retrieveClient()

    try {
      LOG.debug('Fetching attachment metadata', { keys })
      const response = await SELECT.from(attachments, keys).columns("url")

      if (!response?.url) {
        LOG.warn(
          'File URL not found in database', null,
          'Check if the attachment exists and has been properly uploaded',
          { keys, hasResponse: !!response })
        return null
      }

      LOG.debug('Streaming file from Azure Blob Storage', {
        containerName: containerClient.containerName,
        fileId: keys.ID,
        blobName: response.url
      })

      const blobClient = containerClient.getBlockBlobClient(response.url)
      const downloadResponse = await blobClient.download()

      const duration = Date.now() - startTime
      LOG.debug('File streamed from Azure Blob Storage successfully', {
        fileId: keys.ID,
        duration
      })

      return downloadResponse.readableStreamBody
    } catch (error) {
      const duration = Date.now() - startTime
      const suggestion = error.code === 'BlobNotFound' ?
        'File may have been deleted from Azure Blob Storage or URL is incorrect' :
        error.code === 'AuthenticationFailed' ?
          'Check Azure Blob Storage credentials and SAS token' :
          'Check Azure Blob Storage connectivity and configuration'

      LOG.error(
        'File download from Azure Blob Storage failed', error,
        suggestion,
        { fileId: keys?.ID, containerName: containerClient.containerName, attachmentName: attachments.name, duration })

      throw error
    }
  }

  /**
   * Deletes a file from Azure Blob Storage
   * @param {string} Key - The key of the file to delete
   * @returns {Promise} - Promise resolving when deletion is complete
   */
  async delete(blobName) {
    const { containerClient } = await this.retrieveClient()
    LOG.debug(`[Azure] Executing delete for file ${blobName} in bucket ${containerClient.containerName}`)

    const blobClient = containerClient.getBlockBlobClient(blobName)
    const response = await blobClient.delete()
    return response._response.status === 202
  }
}
