package com.opstty.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Q7_phase2_mapper extends Mapper<Object, Text, NullWritable, Text> {
    private int maxTrees = 0;
    private Text districtWithMaxTrees = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] columns = value.toString().split("\t");
        String district = columns[0];
        int numTrees = Integer.parseInt(columns[1]);

        // Check if this district has more trees than the current maximum
        if (numTrees > maxTrees) {
            maxTrees = numTrees;
            districtWithMaxTrees.set(district);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Emit the district with the maximum number of trees
        context.write(NullWritable.get(), districtWithMaxTrees);
    }
}
