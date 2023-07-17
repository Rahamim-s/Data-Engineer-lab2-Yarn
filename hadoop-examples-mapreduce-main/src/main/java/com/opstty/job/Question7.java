package com.opstty.job;

import com.opstty.mapper.Q7_mapper;
import com.opstty.mapper.Q7_phase2_mapper;
import com.opstty.reducer.Q7_phase2_reducer;
import com.opstty.reducer.Q7_reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Question7 {
    public static void main(String[] args) throws Exception {
        Configuration conf1 = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: question7 <in> [<in>...] <out>");
            System.exit(2);
        }

        // First Phase: Calculate the number of trees in each district
        Job job1 = Job.getInstance(conf1, "question7_phase1");
        job1.setJarByClass(Question7.class);
        job1.setMapperClass(Q7_mapper.class);
        job1.setCombinerClass(Q7_reducer.class);
        job1.setReducerClass(Q7_reducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job1, new Path(otherArgs[i]));
        }
        Path tempOutputPath = new Path("temp_output");
        FileOutputFormat.setOutputPath(job1, tempOutputPath);
        boolean phase1Success = job1.waitForCompletion(true);

        // Second Phase: Find the district with the maximum number of trees
        if (phase1Success) {
            Configuration conf2 = new Configuration();
            Job job2 = Job.getInstance(conf2, "question7_phase2");
            job2.setJarByClass(Question7.class);
            job2.setMapperClass(Q7_phase2_mapper.class);
            job2.setReducerClass(Q7_phase2_reducer.class);
            job2.setOutputKeyClass(NullWritable.class);
            job2.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job2, tempOutputPath);
            FileOutputFormat.setOutputPath(job2, new Path(otherArgs[otherArgs.length - 1]));
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
    }
}
