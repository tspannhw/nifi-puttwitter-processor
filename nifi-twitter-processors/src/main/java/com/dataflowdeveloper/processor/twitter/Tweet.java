/**
 * 
 */
package com.dataflowdeveloper.processor.twitter;

import twitter4j.GeoLocation;
import twitter4j.Status;
import twitter4j.StatusUpdate;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

/**
 * @author tspann
 *
 */
public class Tweet {

	
	/**
	 * sendTweet
	 * 
	 * @param message
	 * @param latitude
	 * @param longitude
	 * @param consumerKey
	 * @param consumerSecret
	 * @param accessToken
	 * @param accessTokenSecret
	 * @return
	 */
	public String sendTweet(String message, String latitude, String longitude, String consumerKey, String consumerSecret, String accessToken, String accessTokenSecret) {
		// precondition
		if ( message == null || consumerKey == null || consumerSecret == null || accessToken == null || accessTokenSecret == null ) {
			System.err.println("Missing message or keys");
			return "FAILED";
		}
	    Status status = null;
	    
		try {
		// Set Debug Enabled true to return entire JSON reply
	    ConfigurationBuilder cb = new ConfigurationBuilder();
	    cb.setDebugEnabled(false) 
	      .setPrettyDebugEnabled(false)
	      .setOAuthConsumerKey(consumerKey)
	      .setOAuthConsumerSecret(consumerSecret)
	      .setOAuthAccessToken(accessToken)
	      .setOAuthAccessTokenSecret(accessTokenSecret);
	    TwitterFactory tf = new TwitterFactory(cb.build());	    
	    Twitter twitter = tf.getInstance();

			StatusUpdate update = new StatusUpdate(message);
			try {
				if ( latitude != null && longitude != null && Double.parseDouble(latitude) > 0.0d && Double.parseDouble(longitude) > 0.0d) { 					
					GeoLocation location = new GeoLocation(Double.parseDouble(latitude), Double.parseDouble(longitude));
					update.setLocation(location);
				}
			} catch (NumberFormatException e) {

			}
			
			// TODO:  pass in flowfile to post, then check if jpg/png/gif
			// upload media file update.setMedia(file);
			status = twitter.updateStatus(update);			
		} catch (TwitterException e) {
			e.printStackTrace();
			return "FAILED";
		}

	    return status.getId() + "," +  status.getCreatedAt();
	}
}
