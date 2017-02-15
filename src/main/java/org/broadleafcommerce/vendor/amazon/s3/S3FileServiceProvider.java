/*
 * #%L
 * BroadleafCommerce Amazon Integrations
 * %%
 * Copyright (C) 2009 - 2014 Broadleaf Commerce
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *       http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package org.broadleafcommerce.vendor.amazon.s3;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Resource;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.broadleafcommerce.common.file.FileServiceException;
import org.broadleafcommerce.common.file.domain.FileWorkArea;
import org.broadleafcommerce.common.file.service.BroadleafFileService;
import org.broadleafcommerce.common.file.service.FileServiceProvider;
import org.broadleafcommerce.common.file.service.type.FileApplicationType;
import org.broadleafcommerce.common.site.domain.Site;
import org.broadleafcommerce.common.web.BroadleafRequestContext;
import org.springframework.stereotype.Service;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.DeleteObjectsResult.DeletedObject;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.MultiObjectDeleteException;
import com.amazonaws.services.s3.model.MultiObjectDeleteException.DeleteError;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

@Service("blS3FileServiceProvider")
/**
 * Provides an Amazon S3 compatible implementation of the FileServiceProvider interface.
 *
 * Uses the <code>blS3ConfigurationService</code> component to provide the amazon connection details.   Once a 
 * resource is retrieved from Amazon, the resulting input stream is written to a File on the local file system using
 * <code>blFileService</code> to determine the local file path.
 *    
 * @author bpolster
 *
 */
public class S3FileServiceProvider implements FileServiceProvider {
    protected static final Log LOG = LogFactory.getLog(S3FileServiceProvider.class);

    @Resource(name = "blS3ConfigurationService")
    protected S3ConfigurationService s3ConfigurationService;

    @Resource(name = "blFileService")
    protected BroadleafFileService blFileService;

    protected Map<S3Configuration, AmazonS3Client> configClientMap = new HashMap<S3Configuration, AmazonS3Client>();

    @Override
    public File getResource(String name) {
        return getResource(name, FileApplicationType.ALL);
    }

    @Override
    public File getResource(String name, FileApplicationType fileApplicationType) {
        final S3Configuration s3config = s3ConfigurationService.lookupS3Configuration();
        final String resourceName = buildResourceName(s3config, name);
        final File returnFile = blFileService.getLocalResource(resourceName);
        final String s3Uri = String.format("s3://%s/%s", s3config.getDefaultBucketName(), resourceName);

        OutputStream outputStream = null;
        InputStream inputStream = null;

        try {
            final AmazonS3Client s3 = getAmazonS3Client(s3config);
            final S3Object object = s3.getObject(new GetObjectRequest(s3config.getDefaultBucketName(), buildResourceName(s3config, name)));

            if (LOG.isTraceEnabled()) {
                LOG.trace("retrieving " + s3Uri);
            }
            inputStream = object.getObjectContent();

            if (!returnFile.getParentFile().exists()) {
                if (!returnFile.getParentFile().mkdirs()) {
                    // Other thread could have created - check one more time.
                    if (!returnFile.getParentFile().exists()) {
                        throw new RuntimeException("Unable to create parent directories for file: " + name);
                    }
                }
            }
            outputStream = new FileOutputStream(returnFile);
            int read = 0;
            byte[] bytes = new byte[1024];

            while ((read = inputStream.read(bytes)) != -1) {
                outputStream.write(bytes, 0, read);
            }
        } catch (IOException ioe) {
            throw new RuntimeException(String.format("Error writing %s to local file system at %s", s3Uri, returnFile.getAbsolutePath()), ioe);
        } catch (AmazonS3Exception s3Exception) {
            LOG.error(String.format("%s for %s; name = %s, resourceName = %s, returnFile = %s",
                    s3Exception.getErrorCode(),
                    s3Uri,
                    name,
                    resourceName,
                    returnFile.getAbsolutePath()));

            if ("NoSuchKey".equals(s3Exception.getErrorCode())) {
                //return new File("this/path/should/not/exist/" + UUID.randomUUID());
                return null;
            } else {
                throw s3Exception;
            }
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    throw new RuntimeException("Error closing input stream while writing s3 file to file system", e);
                }
            }
            if (outputStream != null) {
                try {
                    outputStream.close();
                } catch (IOException e) {
                    throw new RuntimeException("Error closing output stream while writing s3 file to file system", e);
                }

            }
        }
        return returnFile;
    }

    @Override
    public void addOrUpdateResources(FileWorkArea workArea, List<File> files, boolean removeFilesFromWorkArea) {
        addOrUpdateResourcesForPaths(workArea, files, removeFilesFromWorkArea);
    }

    /**
     * Writes the resource to S3.   If the bucket returns as "NoSuchBucket" then will attempt to create the bucket
     * and try again.
     */
    @Override
    public List<String> addOrUpdateResourcesForPaths(FileWorkArea workArea, List<File> files, boolean removeFilesFromWorkArea) {
        S3Configuration s3config = s3ConfigurationService.lookupS3Configuration();
        AmazonS3Client s3 = getAmazonS3Client(s3config);

        try {
            return addOrUpdateResourcesInternal(s3config, s3, workArea, files, removeFilesFromWorkArea);
        } catch (AmazonServiceException ase) {
            if ("NoSuchBucket".equals(ase.getErrorCode())) {
                s3.createBucket(s3config.getDefaultBucketName());
                return addOrUpdateResourcesInternal(s3config, s3, workArea, files, removeFilesFromWorkArea);
            } else {
                throw new RuntimeException(ase);
            }
        }
    }

    protected List<String> addOrUpdateResourcesInternal(S3Configuration s3config,
            AmazonS3Client s3,
            FileWorkArea workArea,
            List<File> files,
            boolean removeFilesFromWorkArea) {
        final List<String> resourcePaths = new ArrayList<String>();
        for (final File srcFile : files) {
            if (!srcFile.getAbsolutePath().startsWith(workArea.getFilePathLocation())) {
                throw new FileServiceException(
                        "Attempt to update file " + srcFile.getAbsolutePath() + " that is not in the passed in WorkArea " + workArea.getFilePathLocation());
            }
            final long ts1 = System.currentTimeMillis();
            final String fileName = srcFile.getAbsolutePath().substring(workArea.getFilePathLocation().length());
            final String resourceName = buildResourceName(s3config, fileName);

            ObjectMetadata meta = null;
            try {
                final GetObjectMetadataRequest get = new GetObjectMetadataRequest(s3config.getDefaultBucketName(), resourceName);
                meta = s3.getObjectMetadata(get);
            } catch (AmazonS3Exception ex) {
                meta = null;
            }
            final long ts2 = System.currentTimeMillis();

            if (meta == null || meta.getContentLength() != srcFile.length()) {
            	final PutObjectRequest put = new PutObjectRequest(s3config.getDefaultBucketName(), resourceName, srcFile);

                if ((s3config.getStaticAssetFileExtensionPattern() != null)
                        && s3config.getStaticAssetFileExtensionPattern().matcher(getExtension(fileName)).matches()) {
                    put.setCannedAcl(CannedAccessControlList.PublicRead);
                }

                s3.putObject(put);
                final long ts3 = System.currentTimeMillis();

                if (LOG.isTraceEnabled()) {
                    final String s3Uri = String.format("s3://%s/%s", s3config.getDefaultBucketName(), resourceName);
                    final String msg = String.format("%s copied/updated to %s; queryTime = %dms; uploadTime = %dms; totalTime = %dms",
                            srcFile.getAbsolutePath(),
                            s3Uri,
                            ts2 - ts1,
                            ts3 - ts2,
                            ts3 - ts1);

                    LOG.trace(msg);
                }
            } else {
                if (LOG.isTraceEnabled()) {
                    final String s3Uri = String.format("s3://%s/%s", s3config.getDefaultBucketName(), resourceName);
                    final String msg = String.format("%s already at %s with same filesize = %dbytes; queryTime = %dms",
                            srcFile.getAbsolutePath(),
                            s3Uri,
                            srcFile.length(),
                            ts2 - ts1);

                    LOG.trace(msg);
                }
            }

            resourcePaths.add(fileName);
        }
        return resourcePaths;
    }

    public void addOrUpdateResource(InputStream inputStream, String fileName, long fileSizeInBytes) {
        S3Configuration s3config = s3ConfigurationService.lookupS3Configuration();
        AmazonS3Client s3 = getAmazonS3Client(s3config);

        try {
            addOrUpdateResourcesInternalStreamVersion(s3config, s3, inputStream, fileName, fileSizeInBytes);
        } catch (AmazonServiceException ase) {
            if ("NoSuchBucket".equals(ase.getErrorCode())) {
                s3.createBucket(s3config.getDefaultBucketName());
                addOrUpdateResourcesInternalStreamVersion(s3config, s3, inputStream, fileName, fileSizeInBytes);
            } else {
                throw new RuntimeException(ase);
            }
        }
    }

    protected void addOrUpdateResourcesInternalStreamVersion(S3Configuration s3config,
            AmazonS3Client s3,
            InputStream inputStream,
            String fileName,
            long fileSizeInBytes) {
        final String bucketName = s3config.getDefaultBucketName();

        final ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(fileSizeInBytes);
        final String resourceName = buildResourceName(s3config, fileName);
        final PutObjectRequest objToUpload = new PutObjectRequest(bucketName, resourceName, inputStream, metadata);

        if ((s3config.getStaticAssetFileExtensionPattern() != null)
                && s3config.getStaticAssetFileExtensionPattern().matcher(getExtension(fileName)).matches()) {
            objToUpload.setCannedAcl(CannedAccessControlList.PublicRead);
        }

        s3.putObject(objToUpload);

        if (LOG.isTraceEnabled()) {
            final String s3Uri = String.format("s3://%s/%s", s3config.getDefaultBucketName(), resourceName);
            final String msg = String.format("%s copied/updated to %s", fileName, s3Uri);

            LOG.trace(msg);
        }
    }

    @Override
    public boolean removeResource(String name) {
        final S3Configuration s3config = s3ConfigurationService.lookupS3Configuration();
        final AmazonS3Client s3 = getAmazonS3Client(s3config);
        final String resourceName = buildResourceName(s3config, name);

        s3.deleteObject(s3config.getDefaultBucketName(), resourceName);

        final File returnFile = blFileService.getLocalResource(resourceName);

        if (returnFile != null) {
            returnFile.delete();

            if (LOG.isTraceEnabled()) {
                final String s3Uri = String.format("s3://%s/%s", s3config.getDefaultBucketName(), resourceName);

                LOG.trace("deleted " + s3Uri);
                LOG.trace("deleted " + returnFile.getAbsolutePath());
            }
        }
        return true;
    }

    /**
     * hook for overriding name used for resource in S3
     * @param name
     * @return
     */
    public String buildResourceName(S3Configuration s3config, String name) {
        // Strip the starting slash to prevent empty directories in S3 as well as required references by // in the
        // public S3 URL
        if (name.startsWith("/")) {
            name = name.substring(1);
        }

        String baseDirectory = s3config.getBucketSubDirectory();
        if (StringUtils.isNotEmpty(baseDirectory)) {
            if (baseDirectory.startsWith("/")) {
                baseDirectory = baseDirectory.substring(1);
            }
        } else {
            // ensure subDirectory is non-null
            baseDirectory = "";
        }
        
        String versionDirectory = s3config.getVersionSubDirectory();
        if (StringUtils.isNotEmpty(versionDirectory)) {
        	baseDirectory = FilenameUtils.concat(baseDirectory, versionDirectory);
        }

        String siteSpecificResourceName = getSiteSpecificResourceName(name);
        return FilenameUtils.concat(baseDirectory, siteSpecificResourceName);
    }

    protected String getSiteSpecificResourceName(String resourceName) {
        BroadleafRequestContext brc = BroadleafRequestContext.getBroadleafRequestContext();
        if (brc != null) {
            Site site = brc.getNonPersistentSite();
            if (site != null) {
                String siteDirectory = getSiteDirectory(site);
                if (resourceName.startsWith("/")) {
                    resourceName = resourceName.substring(1);
                }
                return FilenameUtils.concat(siteDirectory, resourceName);
            }
        }

        return resourceName;
    }

    protected String getSiteDirectory(Site site) {
        String siteDirectory = "site-" + site.getId();
        return siteDirectory;
    }

    protected AmazonS3Client getAmazonS3Client(S3Configuration s3config) {
        AmazonS3Client client = configClientMap.get(s3config);
        if (client == null) {
            client = new AmazonS3Client(getAWSCredentials(s3config));
            client.setRegion(s3config.getDefaultBucketRegion());

            if (s3config.getEndpointURI() != null) {
                client.setEndpoint(s3config.getEndpointURI());
            }
            configClientMap.put(s3config, client);
        }
        return client;
    }

    protected AWSCredentials getAWSCredentials(final S3Configuration s3configParam) {
        return new AWSCredentials() {

            private final S3Configuration s3ConfigVar = s3configParam;

            @Override
            public String getAWSSecretKey() {
                return s3ConfigVar.getAwsSecretKey();
            }

            @Override
            public String getAWSAccessKeyId() {
                return s3ConfigVar.getGetAWSAccessKeyId();
            }
        };
    }

    public void setBroadleafFileService(BroadleafFileService bfs) {
        this.blFileService = bfs;
    }

    private String getExtension(String fileName) {
        int lastExtension = lastExtensionIdx(fileName);
        String ext = null;

        if (lastExtension != -1) {
            ext = fileName.substring(lastExtension + 1).toLowerCase();
        }

        return ext;
    }

    private Integer lastExtensionIdx(String fileName) {
        return (fileName != null) ? fileName.lastIndexOf('.') : -1;
    }
    
    public boolean exists(String srcKey) {
        final S3Configuration s3config = s3ConfigurationService.lookupS3Configuration();
        final AmazonS3Client s3Client = getAmazonS3Client(s3config);
        final String bucketName = s3config.getDefaultBucketName();

        return s3Client.doesObjectExist(bucketName, srcKey);
    }
    
    public void copyObject(String srcKey, String destKey, boolean checkAndSucceedIfAlreadyMoved) {
    	copyOrMoveObjectImpl(srcKey, destKey, false, checkAndSucceedIfAlreadyMoved);
    }
    
    public void moveObject(String srcKey, String destKey, boolean checkAndSucceedIfAlreadyMoved) {
    	copyOrMoveObjectImpl(srcKey, destKey, true, checkAndSucceedIfAlreadyMoved);
    }
    
    private void copyOrMoveObjectImpl(String srcKey, String destKey, boolean move, boolean checkAndSucceedIfAlreadyMoved) {
        final S3Configuration s3config = s3ConfigurationService.lookupS3Configuration();
        final AmazonS3Client s3Client = getAmazonS3Client(s3config);
        final String bucketName = s3config.getDefaultBucketName();
        // copy
        final CopyObjectRequest objToCopy = new CopyObjectRequest(bucketName, srcKey, bucketName, destKey);
        
        if ((s3config.getStaticAssetFileExtensionPattern() != null)
                && s3config.getStaticAssetFileExtensionPattern().matcher(getExtension(destKey)).matches()) {
            objToCopy.setCannedAccessControlList(CannedAccessControlList.PublicRead);
        }
        try {
        	s3Client.copyObject(objToCopy);
        } catch (AmazonS3Exception s3e) {
        	if (s3e.getStatusCode() == 404 && checkAndSucceedIfAlreadyMoved) {
        		// it's not in the srcKey. Check if something is at the destKey
        		if (s3Client.doesObjectExist(bucketName, destKey)) {
        			final String msg = String.format("src(%s) doesn't exist but dest(%s) does, so assuming success", srcKey, destKey); 
        			LOG.warn(msg);
        			return;
        		} else {
        			final String msg = String.format("neither src(%s) or dest(%s) exist", srcKey, destKey); 
                	throw new RuntimeException(msg);
        		}
        	}
        } catch (AmazonClientException e) {
        	throw new RuntimeException("Unable to copy object from: " + srcKey + " to: " + destKey, e);
        }

        if (move) {
	        // delete the old ones in sandbox folder (those with srcKey)
	        DeleteObjectRequest objToDelete = new DeleteObjectRequest(bucketName, srcKey);
	        try {
	        	s3Client.deleteObject(objToDelete);
	        } catch (AmazonClientException e) {
	        	//throw new RuntimeException("Moving objects to production folder but unable to delete old object: " + srcKey, e);
	        	LOG.error("Moving objects to production folder but unable to delete old object: " + srcKey, e);
	        }
        }
    }

	public void deleteMultipleObjects(List<String> listOfKeysToRemove) {
		if (listOfKeysToRemove == null || listOfKeysToRemove.isEmpty()) {
			return;
		}

		S3Configuration s3config = s3ConfigurationService.lookupS3Configuration();
		AmazonS3Client s3Client = getAmazonS3Client(s3config);
		String bucketName = s3config.getDefaultBucketName();

		DeleteObjectsRequest multiObjectDeleteRequest = new DeleteObjectsRequest(bucketName);

		List<KeyVersion> keys = new ArrayList<KeyVersion>();

		for (String targetKey : listOfKeysToRemove) {
			keys.add(new KeyVersion(targetKey));
		}

		multiObjectDeleteRequest.setKeys(keys);

		try {
			DeleteObjectsResult delObjResult = s3Client.deleteObjects(multiObjectDeleteRequest);
			if (LOG.isTraceEnabled()) {
				String s = listOfKeysToRemove.stream().collect(Collectors.joining(",\n\t"));

				LOG.trace(String.format("Successfully deleted %d items:\n\t%s",
						delObjResult.getDeletedObjects().size(), s));
			}
		} catch (MultiObjectDeleteException e) {
			if (LOG.isTraceEnabled()) {
				LOG.trace(String.format("%s \n", e.getMessage()));
				LOG.trace(String.format("No. of objects successfully deleted = %s\n", e.getDeletedObjects().size()));
				LOG.trace(String.format("No. of objects failed to delete = %s\n", e.getErrors().size()));
				LOG.trace(String.format("Printing error data...\n"));
				for (DeleteError deleteError : e.getErrors()) {
					if (LOG.isTraceEnabled()) {
						LOG.trace(String.format("Object Key: %s\t%s\t%s\n", deleteError.getKey(), deleteError.getCode(),
								deleteError.getMessage()));
					}
				}
			}
			throw new RuntimeException("No. of objects failed to delete = " + e.getErrors().size(), e);
		}
	}

	// from StreamUtils.writeStreamToStream
    private Long writeStreamToStream(InputStream srcStream, OutputStream destStream, int blockSize) throws IOException {
        byte[] byteBuff = new byte[blockSize];
        int count = 0;
        Long totalBytesWritten = 0L;

        while ((count = srcStream.read(byteBuff, 0, byteBuff.length)) > 0) {
            destStream.write(byteBuff, 0, count);
            totalBytesWritten += count;
        }

        destStream.flush();

        return totalBytesWritten;
    }

}