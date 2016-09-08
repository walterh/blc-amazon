/*
 * #%L
 * BroadleafCommerce Amazon Integrations
 * %%
 * Copyright (C) 2009 - 2016 Broadleaf Commerce
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

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Test;

public class S3DefaultRegionTest extends AbstractS3Test {
	final String expectedEndpoint = "https://s3-us-west-2.amazonaws.com";
	final String westRegion = "us-west-2";
	final String defaultV4EndpointUri = "https://s3.amazonaws.com";

	@Test
	public void regionTest1() throws IOException {
		final S3Configuration s3config = new S3Configuration();
		s3config.setDefaultBucketRegion(westRegion);
		s3config.setEndpointURI(defaultV4EndpointUri);

		assertTrue(
				String.format("expected endpointURI=%s, found %s instead", expectedEndpoint, s3config.getEndpointURI()),
				s3config.getEndpointURI().compareTo(expectedEndpoint) == 0);
	}

	@Test
	public void regionTest2() throws IOException {
		// do the other way
		final S3Configuration s3config = new S3Configuration();
		s3config.setEndpointURI(defaultV4EndpointUri);
		s3config.setDefaultBucketRegion(westRegion);

		assertTrue(
				String.format("expected endpointURI=%s, found %s instead", expectedEndpoint, s3config.getEndpointURI()),
				s3config.getEndpointURI().compareTo(expectedEndpoint) == 0);
	}
}
