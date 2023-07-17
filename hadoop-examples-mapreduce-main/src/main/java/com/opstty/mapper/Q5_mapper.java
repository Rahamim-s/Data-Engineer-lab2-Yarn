package com.opstty.mapper;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Q5_mapper extends Mapper<Object, Text, FloatWritable, Text> {
    private final FloatWritable height = new FloatWritable();
    private final Text line = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] columns = value.toString().split(";");
        if (!columns[6].isEmpty() && isNumeric(columns[6])) {
            float treeHeight = Float.parseFloat(columns[6]);
            height.set(treeHeight);
            line.set(value);
            context.write(height, line);
        }
    }

    private boolean isNumeric(String str) {
        try {
            Float.parseFloat(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
