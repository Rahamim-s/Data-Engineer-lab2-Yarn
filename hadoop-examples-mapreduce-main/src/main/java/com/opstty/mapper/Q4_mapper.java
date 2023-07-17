package com.opstty.mapper;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Q4_mapper extends Mapper<Object, Text, Text, FloatWritable> {
    private Text treeSpecies = new Text();
    private FloatWritable treeHeight = new FloatWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        // Ignore the first row in every mapper
        if (key.equals(0)) {
            return;
        }

        String[] columns = line.split("\t");

        if (columns.length < 7) {
            return;
        }

        String species = columns[3];
        String heightStr = columns[6];

        if (heightStr.isEmpty()) {
            return;
        }

        float height = Float.parseFloat(heightStr);

        treeSpecies.set(species);
        treeHeight.set(height);
        context.write(treeSpecies, treeHeight);
    }
}
