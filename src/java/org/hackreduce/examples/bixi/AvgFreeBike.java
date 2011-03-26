package org.hackreduce.examples.bixi;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hackreduce.mappers.BixiMapper;
import org.hackreduce.mappers.XMLInputFormat;
import org.hackreduce.mappers.XMLRecordReader;
import org.hackreduce.models.BixiRecord;

/**
 * This MapReduce job will count the average number of Bixis records in the data dump.
 *
 */
public class AvgFreeBike extends Configured implements Tool {

	public enum Count {
		TOTAL_RECORDS,
		STATION_IDS,
		UNIQUE_KEYS
	}

	public static class AvgFreeBikeMapper extends BixiMapper<Text, DoubleWritable> {

		// Our own made up key to send all counts to a single Reducer, so we can
		// aggregate a total value.
		public static final Text TOTAL_COUNT = new Text("total");

		// Just to save on object instantiation
		public static final LongWritable ONE_COUNT = new LongWritable(1);

		@Override
			protected void map(BixiRecord record, Context context) throws IOException,
			InterruptedException {

			context.getCounter(Count.TOTAL_RECORDS).increment(1);
			context.write(new Text(record.getRecordDateDay()+"_"+record.getRecordDateHour()+"_"+record.getStationId()),
						  // new DoubleWritable(record.getNbBikes() + record.getNbEmptyDocks()));
						  new DoubleWritable(record.getNbBikes()));
		}
	}

	public static class AvgFreeBikeReducer extends Reducer<Text, DoubleWritable, Text, Text> {

		@Override
			protected void reduce(Text key,
								  Iterable<DoubleWritable> values,
								  Context context)
			throws IOException, InterruptedException {
			context.getCounter(Count.STATION_IDS).increment(1);

			double avg = 0.0;
			int n = 0;
			for (DoubleWritable value : values) {
				avg = (avg * n + value.get()) / ++n;
			}

			context.write(key, new Text(Double.toString(avg)));
		}

	}

	public void configureJob(Job job) {
		// The BIXI datasets are XML files with each station information enclosed withing
		// the <station></station> tags
		job.setInputFormatClass(XMLInputFormat.class);
		XMLRecordReader.setRecordTags(job, "<station>", "</station>");
	}

	@Override
		public int run(String[] args) throws Exception {
		Configuration conf = getConf();

		if (args.length != 2) {
			System.err.println("Usage: " + getClass().getName() + " <input> <output>");
			System.exit(2);
		}

		// Creating the MapReduce job (configuration) object
		Job job = new Job(conf);
		job.setJarByClass(getClass());
		job.setJobName(getClass().getName());

		// Tell the job which Mapper and Reducer to use (classes defined above)
		job.setMapperClass(AvgFreeBikeMapper.class);
		job.setReducerClass(AvgFreeBikeReducer.class);

		configureJob(job);

		// This is what the Mapper will be outputting to the Reducer
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		// This is what the Reducer will be outputting
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Double.class);

		// Setting the input folder of the job
		FileInputFormat.addInputPath(job, new Path(args[0]));

		// Preparing the output folder by first deleting it if it exists
		Path output = new Path(args[1]);
		FileSystem.get(conf).delete(output, true);
		FileOutputFormat.setOutputPath(job, output);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new AvgFreeBike(), args);
		System.exit(result);
	}

}
