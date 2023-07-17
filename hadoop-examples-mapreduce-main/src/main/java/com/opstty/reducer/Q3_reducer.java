package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Q3_reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable totalTrees = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable value : values) {
            count += value.get();
        }
        totalTrees.set(count);
        context.write(key, totalTrees);
    }
}
