package com.opstty.reducer;

import com.opstty.job.TreeWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Q6_reducer extends Reducer<Text, TreeWritable, Text, Text> {
    private final Text result = new Text();

    public void reduce(Text key, Iterable<TreeWritable> values, Context context) throws IOException, InterruptedException {
        int oldestAge = Integer.MAX_VALUE;

        for (TreeWritable value : values) {
            int age = value.getAge();
            if (age < oldestAge) {
                oldestAge = age;
            }
        }

        // Set the result text with the district and the oldest tree's age
        result.set("District: " + key.toString() + ", Oldest tree's age: " + oldestAge);

        // Emit the result
        context.write(key, result);
    }
}
