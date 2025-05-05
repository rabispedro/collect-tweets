package com.brcme.collector.tweets;

import java.net.UnknownHostException;

public class Main {
	private ConfigurationBuilder configurationBuilder;
	private DB dataBase;
	private DBCollection collection;

	public static void main(String[] args) throws InterruptedException {
		Main tw = new Main();

		tw.conectaMongoDB();
		tw.configuraCredenciais();
		tw.capturaTweets();
	}
	
	public void capturaTweets() throws InterruptedException {
		TwitterStream twitterStream = new TwitterStreamFactory(configurationBuilder.build()).getInstance();

		StatusListener listener = new StatusListener() {
			@Override
			public void onStatus(Status status) {
				BasicDBObject obj = new BasicDBObject();

				obj.put("tweet_id", status.getId());
				obj.put("user", status.getUser().getScreenName());
				obj.put("tweet", status.getText());

				try {
					collection.insert(obj);
				} catch (Exception e) {
					System.out.println("Erro de conexão: " + e.getMessage());
				}
			}
		};

		String[] keywords = { "lady-gaga", "rio-de-janeiro", "copacabana" };

		FilterQuery fq = new FilterQuery();
		fq.track(keywords);

		twitterStream.addListener(listener);
		twitterStream.filter(fq);
	}
	
	public void configuraCredenciais() {
		configurationBuilder = new ConfigurationBuilder();

		configurationBuilder.setDebugEnabled(true);
		configurationBuilder.setOAuthConsumerKey("xxxx-xxxx-xxxx-xxxx");
		configurationBuilder.setOAuthConsumerSecret("xxxx-xxxx-xxxx-xxxx");
		configurationBuilder.setOAuthAccessToken("xxxx-xxxx-xxxx-xxxx");
		configurationBuilder.setOAuthAccessTokenSecret("xxxx-xxxx-xxxx-xxxx");
	}

	public void conectaMongoDB() {
		try {
			Mongo mongoCli = new MongoClient("127.0.0.1");
			
			dataBase = mongoCli.getDB("twitter_db");
			collection = dataBase.getCollection("tweets");
			BasicDBObject index = new BasicDBObject("tweet_id", 1);

			collection.ensureIndex(index, new BasicDBObject("unique", true));
		} catch (UnknownHostException | Exception ex) {
			System.out.println("MongoException: " + ex.getMessage());
		}
	}
}