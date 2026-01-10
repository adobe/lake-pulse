# Lake Pulse Storage Support

<table>
  <thead>
    <tr>
      <th>Storage Backend</th>
      <th style="text-align: center;">Support</th>
      <th>Notes</th>
    </tr>
  </thead>
  <tbody>
    <!-- Standard Protocols -->
    <tr>
      <td colspan="3"><strong>Standard Protocols</strong></td>
    </tr>
    <tr>
      <td>HTTP/WebDAV</td>
      <td style="text-align: center;">✅</td>
      <td>WebDAV extends HTTP with file operations (RFC 2518)</td>
    </tr>
    <tr>
      <td>FTP</td>
      <td style="text-align: center;">❌</td>
      <td></td>
    </tr>
    <tr>
      <td>SFTP/SSH</td>
      <td style="text-align: center;">❌</td>
      <td></td>
    </tr>
    <!-- Major Cloud Object Storage -->
    <tr>
      <td colspan="3"><strong>Major Cloud Object Storage</strong></td>
    </tr>
    <tr>
      <td>AWS S3 (and compatible)</td>
      <td style="text-align: center;">✅</td>
      <td></td>
    </tr>
    <tr>
      <td>Azure Blob Storage</td>
      <td style="text-align: center;">✅</td>
      <td></td>
    </tr>
    <tr>
      <td>Azure Data Lake Storage Gen2</td>
      <td style="text-align: center;">✅</td>
      <td>Supports both standard and Fabric endpoints</td>
    </tr>
    <tr>
      <td>Google Cloud Storage</td>
      <td style="text-align: center;">✅</td>
      <td></td>
    </tr>
    <!-- Other Cloud Object Storage -->
    <tr>
      <td colspan="3"><strong>Other Cloud Object Storage</strong></td>
    </tr>
    <tr>
      <td>Alibaba Cloud OSS</td>
      <td style="text-align: center;">✅</td>
      <td>Use AWS config with custom <code>endpoint</code></td>
    </tr>
    <tr>
      <td>Backblaze B2</td>
      <td style="text-align: center;">✅</td>
      <td>Use AWS config with custom <code>endpoint</code></td>
    </tr>
    <tr>
      <td>Ceph</td>
      <td style="text-align: center;">✅</td>
      <td>Use AWS config with custom <code>endpoint</code></td>
    </tr>
    <tr>
      <td>Cloudflare R2</td>
      <td style="text-align: center;">✅</td>
      <td>Use AWS config with custom <code>endpoint</code></td>
    </tr>
    <tr>
      <td>Huawei OBS</td>
      <td style="text-align: center;">✅</td>
      <td>Use AWS config with custom <code>endpoint</code></td>
    </tr>
    <tr>
      <td>MinIO</td>
      <td style="text-align: center;">✅</td>
      <td>Use AWS config with custom <code>endpoint</code></td>
    </tr>
    <tr>
      <td>OpenStack Swift</td>
      <td style="text-align: center;">✅</td>
      <td>Use AWS config with custom <code>endpoint</code></td>
    </tr>
    <tr>
      <td>Tencent Cloud COS</td>
      <td style="text-align: center;">✅</td>
      <td>Use AWS config with custom <code>endpoint</code></td>
    </tr>
    <tr>
      <td>Upyun</td>
      <td style="text-align: center;">❌</td>
      <td></td>
    </tr>
    <tr>
      <td>Vercel Blob</td>
      <td style="text-align: center;">❌</td>
      <td></td>
    </tr>
    <!-- File Systems -->
    <tr>
      <td colspan="3"><strong>File Systems</strong></td>
    </tr>
    <tr>
      <td>Local filesystem</td>
      <td style="text-align: center;">✅</td>
      <td></td>
    </tr>
    <tr>
      <td>HDFS</td>
      <td style="text-align: center;">✅</td>
      <td>Via <code>hdfs-native-object-store</code> crate</td>
    </tr>
    <tr>
      <td>Alluxio</td>
      <td style="text-align: center;">❌</td>
      <td></td>
    </tr>
    <tr>
      <td>Azure Files</td>
      <td style="text-align: center;">❌</td>
      <td></td>
    </tr>
    <tr>
      <td>Databricks DBFS</td>
      <td style="text-align: center;">❌</td>
      <td></td>
    </tr>
    <tr>
      <td>IPFS</td>
      <td style="text-align: center;">❌</td>
      <td></td>
    </tr>
    <tr>
      <td>MongoDB GridFS</td>
      <td style="text-align: center;">❌</td>
      <td></td>
    </tr>
    <tr>
      <td>WebHDFS</td>
      <td style="text-align: center;">❌</td>
      <td></td>
    </tr>
    <!-- Consumer Cloud Storage -->
    <tr>
      <td colspan="3"><strong>Consumer Cloud Storage</strong></td>
    </tr>
    <tr>
      <td>Aliyun Drive</td>
      <td style="text-align: center;">❌</td>
      <td></td>
    </tr>
    <tr>
      <td>Dropbox</td>
      <td style="text-align: center;">❌</td>
      <td></td>
    </tr>
    <tr>
      <td>Google Drive</td>
      <td style="text-align: center;">❌</td>
      <td></td>
    </tr>
    <tr>
      <td>Koofr</td>
      <td style="text-align: center;">❌</td>
      <td></td>
    </tr>
    <tr>
      <td>OneDrive</td>
      <td style="text-align: center;">❌</td>
      <td></td>
    </tr>
    <tr>
      <td>pCloud</td>
      <td style="text-align: center;">❌</td>
      <td></td>
    </tr>
    <tr>
      <td>Seafile</td>
      <td style="text-align: center;">❌</td>
      <td></td>
    </tr>
    <tr>
      <td>Yandex Disk</td>
      <td style="text-align: center;">❌</td>
      <td></td>
    </tr>
  </tbody>
</table>
