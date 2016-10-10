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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.broadleafcommerce.common.config.service.SystemPropertiesService;
import org.springframework.stereotype.Service;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.google.common.base.Strings;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

/**
 * Service that returns the an S3 configuration object. Returns a configuration
 * object with values that are defined as system properties.
 * 
 * @author bpolster
 *
 */
@Service("blS3ConfigurationService")
public class S3ConfigurationServiceImpl implements S3ConfigurationService {
	protected static final Log LOG = LogFactory.getLog(S3ConfigurationServiceImpl.class);

	@Resource(name = "blSystemPropertiesService")
	protected SystemPropertiesService systemPropertiesService;

	protected S3Configuration s3config;
	
	private void initS3ConfigurationImpl() {
    	final long ts1 = System.currentTimeMillis();
		s3config = new S3Configuration();
		s3config.setAwsSecretKey(lookupProperty("aws.s3.secretKey"));
		s3config.setDefaultBucketName(lookupProperty("aws.s3.defaultBucketName"));
		s3config.setDefaultBucketRegion(RegionUtils.getRegion(lookupProperty("aws.s3.defaultBucketRegion")));
		s3config.setGetAWSAccessKeyId(lookupProperty("aws.s3.accessKeyId"));
		s3config.setEndpointURI(lookupProperty("aws.s3.endpointURI"));
		s3config.setBucketSubDirectory(lookupProperty("aws.s3.bucketSubDirectory"));

		final String staticAssetFileExtensionPatternStr = lookupProperty("aws.s3.staticAssetFileExtensionPattern");
		if (!Strings.isNullOrEmpty(staticAssetFileExtensionPatternStr)) {
			s3config.setStaticAssetFileExtensionPattern(staticAssetFileExtensionPatternStr);
		}

		final boolean accessSecretKeyBlank = StringUtils.isEmpty(s3config.getAwsSecretKey());
		final boolean accessKeyIdBlank = StringUtils.isEmpty(s3config.getGetAWSAccessKeyId());
		final boolean bucketNameBlank = StringUtils.isEmpty(s3config.getDefaultBucketName());
		final Region region = s3config.getDefaultBucketRegion();		
        final long ts2 = System.currentTimeMillis();
        
		if (LOG.isTraceEnabled()) {
			final String msg = String.format("%s - using s3://%s/%s in region %s; setup time = %dms", s3config.getEndpointURI(),
					s3config.getDefaultBucketName(), s3config.getBucketSubDirectory(), region.toString(), ts2 - ts1);
			LOG.trace(msg);
		}

		if (region == null || accessSecretKeyBlank || accessKeyIdBlank || bucketNameBlank) {
			StringBuilder errorMessage = new StringBuilder("Amazon S3 Configuration Error : ");

			if (accessSecretKeyBlank) {
				errorMessage.append("aws.s3.secretKey was blank,");
			}

			if (accessKeyIdBlank) {
				errorMessage.append("aws.s3.accessKeyId was blank,");
			}

			if (bucketNameBlank) {
				errorMessage.append("aws.s3.defaultBucketName was blank,");
			}

			if (region == null) {
				errorMessage.append("aws.s3.defaultBucketRegion was set to an invalid value of "
						+ s3config.getDefaultBucketRegion());
			}
			throw new IllegalArgumentException(errorMessage.toString());
		}
	}

	@Override
	public S3Configuration lookupS3Configuration() {
		if (s3config == null) {
			initS3ConfigurationImpl(); 
		}
		return s3config;
	}

	protected String lookupProperty(String propertyName) {
		return systemPropertiesService.resolveSystemProperty(propertyName);
	}

	protected void setSystemPropertiesService(SystemPropertiesService systemPropertiesService) {
		this.systemPropertiesService = systemPropertiesService;
	}

}
