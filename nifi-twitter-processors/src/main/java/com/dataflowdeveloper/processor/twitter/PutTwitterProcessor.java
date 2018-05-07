/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dataflowdeveloper.processor.twitter;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;


@Tags({ "PutTwitter", "tweet", "twitter", "tweets", "social media", "status", "json" })
@CapabilityDescription("SEnd a tweet")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "consumerKey", description = "OAUTH Twitter Consumer Key") })
@WritesAttributes({ @WritesAttribute(attribute = "tweetStatus", description = "output from twitter") })
public class PutTwitterProcessor extends AbstractProcessor {

	public static final PropertyDescriptor CONSUMER_KEY = new PropertyDescriptor.Builder().name("Consumer Key")
			.description("The Consumer Key provided by Twitter").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
	public static final PropertyDescriptor CONSUMER_SECRET = new PropertyDescriptor.Builder().name("Consumer Secret")
			.description("The Consumer Secret provided by Twitter").required(true).sensitive(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
	public static final PropertyDescriptor ACCESS_TOKEN = new PropertyDescriptor.Builder().name("Access Token")
			.description("The Access Token provided by Twitter").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
	public static final PropertyDescriptor ACCESS_TOKEN_SECRET = new PropertyDescriptor.Builder()
			.name("Access Token Secret").description("The Access Token Secret provided by Twitter").required(true)
			.sensitive(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
	public static final PropertyDescriptor MESSAGE = new PropertyDescriptor.Builder().name("Message")
			.expressionLanguageSupported(true)
			.description("Message to post to status on Twitter").required(true).sensitive(false)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
	public static final PropertyDescriptor LATITUDE = new PropertyDescriptor.Builder().name("latitude")
			.displayName("Latitude")
			.addValidator(StandardValidators.NON_BLANK_VALIDATOR)
			.description("Geolocation Latitude to post to status on Twitter").required(false)
			.expressionLanguageSupported(true)
			.build();
	public static final PropertyDescriptor LONGITUDE = new PropertyDescriptor.Builder().name("longitude")
			.displayName("Longitude")
			.expressionLanguageSupported(true)
			.addValidator(StandardValidators.NON_BLANK_VALIDATOR)
			.description("Geolocation Longitude to post to status on Twitter").required(false)
			.build();
	
	public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
			.description("All status updates will be routed to this relationship").build();

	public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
			.description("Failed to determine image.").build();

	private List<PropertyDescriptor> descriptors;

	private Set<Relationship> relationships;

	private Tweet tweet;
	
	@Override
	protected void init(final ProcessorInitializationContext context) {
		final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
		descriptors.add(CONSUMER_KEY);
		descriptors.add(CONSUMER_SECRET);
		descriptors.add(ACCESS_TOKEN);
		descriptors.add(ACCESS_TOKEN_SECRET);
		descriptors.add(MESSAGE);
		descriptors.add(LATITUDE);
		descriptors.add(LONGITUDE);
		this.descriptors = Collections.unmodifiableList(descriptors);

		final Set<Relationship> relationships = new HashSet<Relationship>();
		relationships.add(REL_SUCCESS);
		relationships.add(REL_FAILURE);
		this.relationships = Collections.unmodifiableSet(relationships);
	}

	@Override
	public Set<Relationship> getRelationships() {
		return this.relationships;
	}

	@Override
	public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return descriptors;
	}

	@OnScheduled
	public void onScheduled(final ProcessContext context) {
		tweet = new Tweet();
		return;
	}

	// @See https://github.com/apache/nifi/blob/d838f61291d2582592754a37314911b701c6891b/nifi-nar-bundles/nifi-social-media-bundle/nifi-twitter-processors/src/main/java/org/apache/nifi/processors/twitter/GetTwitter.java
	
	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		FlowFile flowFile = session.get();
		if (flowFile == null) {
			flowFile = session.create();
		}
		try {
			flowFile.getAttributes();

			final String consumerKey = context.getProperty(CONSUMER_KEY).getValue();
			final String consumerSecret = context.getProperty(CONSUMER_SECRET).getValue();
			final String accessToken = context.getProperty(ACCESS_TOKEN).getValue();
			final String accessTokenSecret = context.getProperty(ACCESS_TOKEN_SECRET).getValue();

			final String message = context.getProperty(MESSAGE).evaluateAttributeExpressions(flowFile).getValue();
			final String latitude = context.getProperty(LATITUDE).evaluateAttributeExpressions(flowFile).getValue();
			final String longitude = context.getProperty(LONGITUDE).evaluateAttributeExpressions(flowFile).getValue();

			String reply = tweet.sendTweet(message, latitude, longitude, consumerKey, consumerSecret, accessToken,
					accessTokenSecret);

			try {
				final HashMap<String, String> attributes = new HashMap<String, String>();

				session.read(flowFile, new InputStreamCallback() {
					@Override
					public void process(InputStream input) throws IOException {
						// when we want to add an image
//						byte[] byteArray = IOUtils.toByteArray(input);
//						getLogger().debug(
//								String.format("read %d bytes from incoming file", new Object[] { byteArray.length }));
						

						if (reply != null) {
							getLogger().debug(reply);
							attributes.put("PutTwitterStatus",reply);
						}
					}
				});
				if (attributes.size() == 0) {
					session.transfer(flowFile, REL_FAILURE);
				} else {
					flowFile = session.putAllAttributes(flowFile, attributes);
					session.transfer(flowFile, REL_SUCCESS);
				}
			} catch (Exception e) {
				throw new ProcessException(e);
			}

			session.commit();
		} catch (

		final Throwable t) {
			getLogger().error("Unable to process Put Twitter Processor file " + t.getLocalizedMessage());
			throw new ProcessException(t);
		}
	}
}
