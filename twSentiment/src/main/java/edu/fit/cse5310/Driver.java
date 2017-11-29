package edu.fit.cse5310;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Driver {
    private static String coreSitePath = "/usr/local/hadoop/etc/hadoop/core-site.xml";
    private static Configuration conf = new Configuration();
    private static Path inputFilePath;
    private static final String activeUsersOutput = "/twSentiment/output/activeUsers";
    private static final String retweetedUsersOutput = "/twSentiment/output/retweetedUsers";
    private static final String tweetedHashtagsOutput = "/twSentiment/output/tweetedHashtags";
    private static final String sentimentsOutput = "/twSentiment/output/sentiments";

    public static void main(String[] args) {
        if (args.length == 1) {
            inputFilePath = new Path(args[0]);
            conf.addResource(new Path(coreSitePath));

            runActiveUsersJob(inputFilePath);
            runRetweetedUsersJob(inputFilePath);
            runTweetedHashtagsJob(inputFilePath);
            runSentimentsJob(inputFilePath);
        }
    }

    private static void runActiveUsersJob(Path inputFilePath) {
        try {
            Job job = Job.getInstance(conf, "ActiveUsers");
            job.setJarByClass(ActiveUsers.class);
            job.setMapperClass(ActiveUsers.UserMapper.class);
            job.setCombinerClass(ActiveUsers.IntSumReducer.class);
            job.setReducerClass(ActiveUsers.IntSumReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job, inputFilePath);
            FileSystem.get(conf).delete(new Path(activeUsersOutput), true);
            FileOutputFormat.setOutputPath(job, new Path(activeUsersOutput));
            job.waitForCompletion(true);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static void runRetweetedUsersJob(Path inputFilePath) {
        try {
            Job job = Job.getInstance(conf, "RetweetedUsers");
            job.setJarByClass(RetweetedUsers.class);
            job.setMapperClass(RetweetedUsers.UserMapper.class);
            job.setCombinerClass(RetweetedUsers.IntSumReducer.class);
            job.setReducerClass(RetweetedUsers.IntSumReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job, inputFilePath);
            FileSystem.get(conf).delete(new Path(retweetedUsersOutput), true);
            FileOutputFormat.setOutputPath(job, new Path(retweetedUsersOutput));
            job.waitForCompletion(true);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static void runTweetedHashtagsJob(Path inputFilePath) {
        try {
            Job job = Job.getInstance(conf, "TweetedHashtags");
            job.setJarByClass(TweetedHashtags.class);
            job.setMapperClass(TweetedHashtags.HashtagMapper.class);
            job.setCombinerClass(TweetedHashtags.IntSumReducer.class);
            job.setReducerClass(TweetedHashtags.IntSumReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job, inputFilePath);
            FileSystem.get(conf).delete(new Path(tweetedHashtagsOutput), true);
            FileOutputFormat.setOutputPath(job, new Path(tweetedHashtagsOutput));
            job.waitForCompletion(true);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static void runSentimentsJob(Path inputFilePath) {
        try {
            Job job = Job.getInstance(conf, "Sentiments");
            job.setJarByClass(Sentiments.class);
            job.setMapperClass(Sentiments.SentimentMapper.class);
            job.setCombinerClass(Sentiments.IntSumReducer.class);
            job.setReducerClass(Sentiments.IntSumReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job, inputFilePath);
            FileSystem.get(conf).delete(new Path(sentimentsOutput), true);
            FileOutputFormat.setOutputPath(job, new Path(sentimentsOutput));
            job.waitForCompletion(true);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
