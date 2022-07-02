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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;


public class VgSalesMapper extends Mapper <LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Membaca 1 baris string dengan tanda , sebagai pemisah
        String valueString = value.toString();

        // Memisahkan string menjadi array of string dengan tanda koma sebagai pemisah
        String[] SingleVgData = valueString.split(",");

        // Mengambil data video games, data video games terdapat pada kolom ke 3 atau index 2
        context.write(new Text(SingleVgData[2]), one);
    }
}
