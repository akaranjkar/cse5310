package edu.fit.cse5310;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FavoritedUsers {
    public static class UserMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text user = new Text();
        private IntWritable favorites = new IntWritable();
        public void map(Object key, Text line, Context context) throws IOException, InterruptedException {
            // date, username, text, retweets, favorites, hashtags, mentions, id
            String[] fields = MiscUtils.fieldsFromLine(line.toString());
            if (!fields[1].equals("") && !fields[4].equals("")
                    && !fields[1].equals("username") && !fields[4].equals("favorites")) {
                user.set(fields[1]);
                favorites.set(Integer.parseInt(fields[4]));
                context.write(user, favorites);
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
