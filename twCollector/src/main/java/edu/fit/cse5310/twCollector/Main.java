/* Code to collect tweets using Twitter API
 * Based on code provided by Twitter
 */

package edu.fit.cse5310.twCollector;

import twitter4j.*;
import twitter4j.auth.OAuth2Token;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Map;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class Main {

    // Fetch consumer key and secret from environment variables
    private static final String CONSUMER_KEY = System.getenv("TW_CONSUMER_KEY");
    private static final String CONSUMER_SECRET = System.getenv("TW_CONSUMER_SECRET");

    // 100 tweets per query is the maximum allowed in the API
    private static final int TWEETS_PER_QUERY = 100;

    // Maximum queries allowed. Don't set above 450
    // Can retrieve upto MAX_QUERIES*TWEETS_PER_QUERY tweets
    private static final int MAX_QUERIES = 450;

    private static final String NEW_LINE_SEPARATOR = "\n";
    private static final String COMMA_DELIMITER = ",";

    private static final boolean DEBUG = true;

    // Cleanup text
    public static String cleanText(String text) {
        text = text.replace("\n", "\\n");
        text = text.replace("\r", "\\r");
        text = text.replace("\t", "\\t");
        text = text.replace(",", " ");
        return text;
    }

    // Get oAuth2 bearer token
    public static OAuth2Token getOAuth2Token() {
        OAuth2Token token = null;
        ConfigurationBuilder cb;

        cb = new ConfigurationBuilder();
        cb.setApplicationOnlyAuthEnabled(true);

        cb.setOAuthConsumerKey(CONSUMER_KEY).setOAuthConsumerSecret(CONSUMER_SECRET);

        try {
            token = new TwitterFactory(cb.build()).getInstance().getOAuth2Token();
        } catch (Exception e) {
            System.out.println("Could not get OAuth2 token");
            e.printStackTrace();
            System.exit(0);
        }

        return token;
    }

    // Get authenticated Twitter object
    public static Twitter getTwitter() {
        OAuth2Token token;
        token = getOAuth2Token();

        ConfigurationBuilder cb = new ConfigurationBuilder();

        cb.setApplicationOnlyAuthEnabled(true);

        cb.setOAuthConsumerKey(CONSUMER_KEY);
        cb.setOAuthConsumerSecret(CONSUMER_SECRET);

        cb.setOAuth2TokenType(token.getTokenType());
        cb.setOAuth2AccessToken(token.getAccessToken());

        return new TwitterFactory(cb.build()).getInstance();
    }

    public static void fetchData(String hashTagInput, String outputCSVFileName) throws IOException {

        Hashtable<String, String> hashTags = new Hashtable<String, String>();
        //FileWriter filewriter = null;

        int totalTweets = 0;

        //	This variable is the key to our retrieving multiple blocks of tweets.  In each batch of tweets we retrieve,
        //	we use this variable to remember the LOWEST tweet ID.  Tweet IDs are (java) longs, and they are roughly
        //	sequential over time.  Without setting the MaxId in the query, Twitter will always retrieve the most
        //	recent tweets.  Thus, to retrieve a second (or third or ...) batch of Tweets, we need to set the Max Id
        //	in the query to be one less than the lowest Tweet ID we've seen already.  This allows us to page backwards
        //	through time to retrieve additional blocks of tweets
        long maxID = -1;

        Twitter twitter = getTwitter();

        //String FILE_HEADER = "TimeStamp, UserId, Tweets, Retweets, HashTags";


        FileWriter filewriter = new FileWriter(outputCSVFileName, true);
        //filewriter.append(NEW_LINE_SEPARATOR);
        //filewriter.append(FILE_HEADER);

        try {
            Map<String, RateLimitStatus> rateLimitStatus = twitter.getRateLimitStatus("search");
            RateLimitStatus searchTweetsRateLimit = rateLimitStatus.get("/search/tweets");

            System.out.printf("You have %d calls remaining out of %d, Limit resets in %d seconds\n",
                    searchTweetsRateLimit.getRemaining(),
                    searchTweetsRateLimit.getLimit(),
                    searchTweetsRateLimit.getSecondsUntilReset());

            // Loop to retrieve multiple blocks of tweets from Twitter
            for (int queryNumber = 0; queryNumber < MAX_QUERIES; queryNumber++) {
                if (searchTweetsRateLimit.getRemaining() == 0) {
                    System.out.printf("!!! Sleeping for %d seconds due to rate limits\n", searchTweetsRateLimit.getSecondsUntilReset());
                    Thread.sleep((searchTweetsRateLimit.getSecondsUntilReset() + 2) * 1000l);
                }

                Query q = new Query(hashTagInput);
                q.setCount(TWEETS_PER_QUERY);
                //q.resultType("recent");
                q.setLang("en");

                //	If maxID is -1, then this is our first call and we do not want to tell Twitter what the maximum
                //	tweet id is we want to retrieve.  But if it is not -1, then it represents the lowest tweet ID
                //	we've seen, so we want to start at it-1 (if we start at maxID, we would see the lowest tweet
                //	a second time...
                if (maxID != -1) {
                    q.setMaxId(maxID - 1);
                }

                QueryResult r = twitter.search(q);
                if (r.getTweets().size() == 0) {
                    break; // No more tweets to fetch
                }
                for (Status s : r.getTweets()) {
                    totalTweets++;
                    //	Keep track of the lowest tweet ID.  If you do not do this, you cannot retrieve multiple
                    //	blocks of tweets...
                    if (maxID == -1 || s.getId() < maxID) {
                        maxID = s.getId();
                    }

                    String hashTagString = buildHashtagString(s, hashTagInput);

                    // Print tweet to stdout
                    if (DEBUG) {
                        printTweet(s, hashTagString);
                    }

                    // Write to CSV
                    writeTweetToFile(filewriter, s, hashTagString);
                }

                // Update rate limit
                searchTweetsRateLimit = r.getRateLimitStatus();
            }

        } catch (Exception e) {
            // Catch all
            e.printStackTrace();
        }

        System.out.printf("\n\nA total of %d tweets retrieved\n", totalTweets);
        filewriter.flush();
        filewriter.close();
    }

    private static void printTweet(Status s, String hashTagString) {
        if (s.isRetweet()) {
            System.out.println(s.getCreatedAt().toString() + "," + s.getUser().getScreenName() + "," +
                    cleanText(s.getText()) + "," + "0" + "," + "0" + "," +
                    cleanText(hashTagString) + "," + "0");
            printTweet(s.getRetweetedStatus(), hashTagString);
        }
        else {
            System.out.println(s.getCreatedAt().toString() + "," + s.getUser().getScreenName() + "," +
                    cleanText(s.getText()) + "," + s.getRetweetCount() + "," + s.getFavoriteCount() + "," +
                    cleanText(hashTagString) + "," + "1");
        }
    }

    private static String buildHashtagString(Status s, String hashTagInput) {
        // Build hashtag string
        String hashTagString = new String();
        if (s.getHashtagEntities().length < 1) {
            hashTagString = hashTagInput.replace("#","");
        } else {
            for (HashtagEntity h : s.getHashtagEntities()) {
                hashTagString += h.getText() + " ";
            }
        }
        return hashTagString.trim();
    }

    private static void writeTweetToFile (FileWriter filewriter, Status s, String hashTagString) throws IOException {
        // timestamp, screenName, tweetText, retweetCount, favoriteCount, hashtags, isOriginalContent
        if (s.isRetweet()) {
            filewriter.append(s.getCreatedAt().toString()); // timestamp
            filewriter.append(COMMA_DELIMITER);
            filewriter.append(s.getUser().getScreenName()); // screenName
            filewriter.append(COMMA_DELIMITER);
            filewriter.append(cleanText(s.getText())); // tweetText
            filewriter.append(COMMA_DELIMITER);
            filewriter.append('0'); // retweetCount
            filewriter.append(COMMA_DELIMITER);
            filewriter.append('0'); // favoriteCount
            filewriter.append(COMMA_DELIMITER);
            filewriter.append(cleanText(hashTagString)); // hashtags
            filewriter.append(COMMA_DELIMITER);
            filewriter.append('0');
            filewriter.append(NEW_LINE_SEPARATOR);
            writeTweetToFile(filewriter, s.getRetweetedStatus(), hashTagString); // write original tweet also
        } else {
            filewriter.append(s.getCreatedAt().toString()); // timestamp
            filewriter.append(COMMA_DELIMITER);
            filewriter.append(s.getUser().getScreenName()); // screenName
            filewriter.append(COMMA_DELIMITER);
            filewriter.append(cleanText(s.getText())); // tweetText
            filewriter.append(COMMA_DELIMITER);
            filewriter.append(String.valueOf(s.getRetweetCount())); // retweetCount
            filewriter.append(COMMA_DELIMITER);
            filewriter.append(String.valueOf(s.getFavoriteCount())); // favoriteCount
            filewriter.append(COMMA_DELIMITER);
            filewriter.append(cleanText(hashTagString)); // hashtags
            filewriter.append(COMMA_DELIMITER);
            filewriter.append('1');
            filewriter.append(NEW_LINE_SEPARATOR);
        }
    }

    public static void main(String[] args) throws IOException {
        String[] hashtags = {"#climatechange", "#globalwarming", "#climate", "#earth", "#science", "#change"};
        String outputCSVFileName = args[0];
        for (String hashtag : hashtags) {
            fetchData(hashtag, outputCSVFileName);
        }

    }
}
