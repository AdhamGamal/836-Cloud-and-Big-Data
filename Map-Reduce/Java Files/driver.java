package minStockPackage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//Created By: Mokhtar Adham, ID: 20398545
public class MinCloseStockDriver {

	public static void main(String[] args) {

		try {

			Configuration jobConf = new Configuration();
			Job hadoopJob = new Job(jobConf);

			FileSystem fileSystem = FileSystem.get(jobConf);
			fileSystem.delete(new Path(args[1]));

			hadoopJob.setJarByClass(MinCloseStockDriver.class);
			hadoopJob.setMapperClass(MinCloseStockMapper.class);
			hadoopJob.setNumReduceTasks(1);
			hadoopJob.setReducerClass(MinCloseStockReducer.class);
			hadoopJob.setOutputKeyClass(Text.class);
			hadoopJob.setOutputValueClass(FloatWritable.class);

			FileInputFormat.addInputPath(hadoopJob, new Path(args[0]));
			FileOutputFormat.setOutputPath(hadoopJob, new Path(args[1]));

			hadoopJob.waitForCompletion(true);

		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
	}
}