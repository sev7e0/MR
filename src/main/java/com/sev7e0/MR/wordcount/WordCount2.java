package com.sev7e0.MR.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount2 {
	/**
	 * @Title: main @Description: 定义的driver：封装了mapreduce作业的所有信息 @param @param
	 * args @param @throws Exception @return void @throws
	 */
	public static void main(String[] args) throws Exception {

		// 设置环境变量HADOOP_USER_NAME，其值是root
		// 在本机调试
		// 读取配置文件
		Configuration conf = new Configuration();
//		conf.set("fs.defaultFS", "hdfs://spark01:9000");
//		conf.set("yarn.resourcemanager.hostname", "spark01");

		Path out = new Path(args[1]);
		FileSystem fs = FileSystem.get(conf);

		//判断输出路径是否存在，当路径存在时mapreduce会报错
		if (fs.exists(out)) {
			fs.delete(out, true);
			System.out.println("ouput is exit  will delete");
		}
		
		// 创建任务
		Job job = Job.getInstance(conf, "wordcountDemo");
		// 设置job的主类
		job.setJarByClass(WordCount2.class); // 主类

		// 设置作业的输入路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));

		//设置map的相关参数
		job.setMapperClass(WordCountMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		//设置reduce相关参数
		job.setReducerClass(WordCountReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		//设置作业的输出路径
		FileOutputFormat.setOutputPath(job, out);

		job.setNumReduceTasks(2);

		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
