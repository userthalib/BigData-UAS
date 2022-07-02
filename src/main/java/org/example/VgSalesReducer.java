package org.example;
 /*
    Import library dari Java Package
  */

import java.io.IOException;
/*
    Import library dari Hadoop Package untuk menjalankan fungsi Pembacaan, Penulisan File ke dalam HDFS dan
  menjalankan MapReduce
  */

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;


public class VgSalesReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text t_key, Iterable<IntWritable> values, Context context) throws IOException,
            InterruptedException {

        Text key = t_key;

        int frequencyForCar = 0;

        for (IntWritable val: values) {
            // frequencyForCar = frequencyForCar + val.get()
            frequencyForCar += val.get();
        }
        context.write(key, new IntWritable(frequencyForCar));
    }
}
