package com.opstty.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Q7_mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text district = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Skip the header line in the CSV
        if (key.get() == 0 && value.toString().contains("GEOPOINT")) {
            return;
        }

        String[] columns = value.toString().split(";");
        if (columns.length >= 2) {
            district.set(columns[1]);
            context.write(district, one);
        }
    }
}
