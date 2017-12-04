package edu.fit.cse5310;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RetweetedUsers {
    public static class UserMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text user = new Text();
        private IntWritable retweets = new IntWritable();
        public void map(Object key, Text line, Context context) throws IOException, InterruptedException {
            // timestamp, screenName, tweetText, retweetCount, favoriteCount, hashtags, isOriginalContent
            String[] fields = MiscUtils.fieldsFromLine(line.toString());
            user.set(fields[1]);
            retweets.set(Integer.parseInt(fields[3]));
            context.write(user,retweets);
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value: values) {
                sum += value.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
}
