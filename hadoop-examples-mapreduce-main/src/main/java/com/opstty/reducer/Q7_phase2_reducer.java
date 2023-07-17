package com.opstty.reducer;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Q7_phase2_reducer extends Reducer<NullWritable, Text, NullWritable, Text> {
    private int maxTrees = 0;
    private Text districtWithMaxTrees = new Text();

    public void reduce(NullWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text value : values) {
            String district = value.toString();
            // Check if this district has more trees than the current maximum
            if (district.length() > 0) {
                districtWithMaxTrees.set(district);
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Emit the district with the maximum number of trees as the final output
        context.write(NullWritable.get(), districtWithMaxTrees);
    }
}
