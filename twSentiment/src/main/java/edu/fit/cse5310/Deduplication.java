package edu.fit.cse5310;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Deduplication {
    public static class DeduplicationMapper extends Mapper<Object, Text, Text, IntWritable> {
        private IntWritable one = new IntWritable(1);
        public void map(Object key, Text line, Context context) throws IOException, InterruptedException {
            context.write(line,one);
        }
    }

    public static class NullValueReducer extends Reducer<Text, IntWritable, Text, NullWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key,NullWritable.get());
        }
    }
}
