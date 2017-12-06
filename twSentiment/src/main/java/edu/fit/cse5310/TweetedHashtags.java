package edu.fit.cse5310;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

public class TweetedHashtags {
    public static class HashtagMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text hashtag = new Text();
        private IntWritable one = new IntWritable(1);
        public void map(Object key, Text line, Context context) throws IOException, InterruptedException {
            // date, username, text, retweets, favorites, hashtags, mentions, id
            String[] fields = MiscUtils.fieldsFromLine(line.toString());
            if (!fields[5].equals("") && !fields[5].equals("hashtags")) {
                String hashtags = fields[5].replace("#","");
                StringTokenizer itr = new StringTokenizer(hashtags);
                while (itr.hasMoreTokens()) {
                    hashtag.set(itr.nextToken());
                    context.write(hashtag, one);
                }
            }
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
