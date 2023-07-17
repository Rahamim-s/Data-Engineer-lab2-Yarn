package com.opstty.mapper;

import com.opstty.job.TreeWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Q6_mapper extends Mapper<LongWritable, Text, Text, TreeWritable> {
    private final Text outputKey = new Text();
    private final TreeWritable outputValue = new TreeWritable();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Split the input line by semicolon
        String[] columns = value.toString().split(";");
        if (columns.length == 13) {
            try {
                // Parse the age and district from the columns
                int age = Integer.parseInt(columns[5]);
                int district = Integer.parseInt(columns[1]);

                // Set the output key and value
                outputKey.set(String.valueOf(district));
                outputValue.setAge(age);
                outputValue.setDistrict(district);

                // Emit the key-value pair
                context.write(outputKey, outputValue);
            } catch (NumberFormatException e) {
                // Ignore lines with invalid age or district format
            }
        }
    }
}