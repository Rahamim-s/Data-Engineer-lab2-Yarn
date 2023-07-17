package com.opstty.job;

import com.opstty.mapper.Q6_mapper;
import com.opstty.reducer.Q6_reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Question6 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "question6");
        job.setJarByClass(Question6.class);
        job.setMapperClass(Q6_mapper.class);
        job.setReducerClass(Q6_reducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TreeWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
