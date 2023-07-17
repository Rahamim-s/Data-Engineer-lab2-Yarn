package com.opstty.mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Q1_mapper extends Mapper<Object, Text, Text, IntWritable> {
    private final Text district = new Text();
    private final static IntWritable ONE = new IntWritable(1);

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Ignore the header row
        if (!value.toString().startsWith("GEOPOINT")) {
            String[] columns = value.toString().split(";");
            if (columns.length > 1) {
                String districtName = columns[1];
                district.set(districtName);
                context.write(district, ONE);
            }
        }
    }
}