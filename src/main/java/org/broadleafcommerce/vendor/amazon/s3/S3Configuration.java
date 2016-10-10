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

import java.util.regex.Pattern;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.amazonaws.regions.Region;

/**
 * Class that holds the configuration for connecting to AmazonS3.
 * 
 * @author bpolster
 *
 */
public class S3Configuration {

    private String awsSecretKey;
    private String getAWSAccessKeyId;
    private String defaultBucketName;
    private Region defaultBucketRegion;
    private String endpointURI;
    private String bucketSubDirectory;
    private Pattern staticAssetFileExtensionPattern;

    public String getAwsSecretKey() {
        return awsSecretKey;
    }

    public void setAwsSecretKey(String awsSecretKey) {
        this.awsSecretKey = awsSecretKey;
    }

    public String getGetAWSAccessKeyId() {
        return getAWSAccessKeyId;
    }

    public void setGetAWSAccessKeyId(String getAWSAccessKeyId) {
        this.getAWSAccessKeyId = getAWSAccessKeyId;
    }

    public String getDefaultBucketName() {
        return defaultBucketName;
    }

    public void setDefaultBucketName(String defaultBucketName) {
        this.defaultBucketName = defaultBucketName;
    }

    public Region getDefaultBucketRegion() {
        return defaultBucketRegion;
    }

    public void setDefaultBucketRegion(Region defaultBucketRegion) {
        this.defaultBucketRegion = defaultBucketRegion;
        
        fixupRegionForSignatureVersion4();
    }
        
    public String getEndpointURI() {
        return endpointURI;
    }

    public void setEndpointURI(String endpointURI) {
        this.endpointURI = endpointURI;
        
        fixupRegionForSignatureVersion4();
    }

    public String getBucketSubDirectory() {
        return bucketSubDirectory;
    }

    public void setBucketSubDirectory(String bucketSubDirectory) {
        this.bucketSubDirectory = bucketSubDirectory;
    }
    
    public Pattern getStaticAssetFileExtensionPattern() {
    	return staticAssetFileExtensionPattern;
    }
    
    public void setStaticAssetFileExtensionPattern(String staticAssetFileExtensionPatternStr) {
    	this.staticAssetFileExtensionPattern = Pattern.compile(staticAssetFileExtensionPatternStr);
    }
    

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
            .append(awsSecretKey)
            .append(awsSecretKey)
            .append(defaultBucketRegion)
            .append(defaultBucketRegion)
            .append(endpointURI)
            .append(bucketSubDirectory)
            .build();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof S3Configuration) {
            S3Configuration that = (S3Configuration) obj;
            return new EqualsBuilder()
                .append(this.awsSecretKey, that.awsSecretKey)
                .append(this.defaultBucketName, that.defaultBucketName)
                .append(this.defaultBucketRegion, that.defaultBucketRegion)
                .append(this.getAWSAccessKeyId, that.getAWSAccessKeyId)
                .append(this.endpointURI, that.endpointURI)
                .append(this.bucketSubDirectory, that.bucketSubDirectory)
                .build();
        }
        return false;
    }

    // http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketPUT.html
    // http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingBucket.html#access-bucket-intro
    private void fixupRegionForSignatureVersion4() {
    	// "s3.amazonaws.com" is default US East region, which is no longer compatible with v4 API
    	if (endpointURI != null && endpointURI.endsWith("s3.amazonaws.com")) {
    		if (defaultBucketRegion != null) {
    			// insert this in like http://s3-aws-region.amazonaws.com to make it v4 compliant
    			final int loc = endpointURI.lastIndexOf(".amazonaws.com");
    			
    			if (loc > 0) {
    				// add the extra dot to make it more clear. Account for it with a +1 index
    				endpointURI = String.format("%s-%s.%s", endpointURI.substring(0, loc), defaultBucketRegion, endpointURI.substring(loc + 1));
    			}
    		}
    	}
    }    
}
