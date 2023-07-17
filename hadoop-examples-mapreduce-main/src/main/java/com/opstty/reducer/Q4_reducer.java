package com.opstty.reducer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Q4_reducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    private FloatWritable maxHeight = new FloatWritable();

    public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        float max = Float.MIN_VALUE;
        for (FloatWritable value : values) {
            max = Math.max(max, value.get());
        }
        maxHeight.set(max);
        context.write(key, maxHeight);
    }
}
