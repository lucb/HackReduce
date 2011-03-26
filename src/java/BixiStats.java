import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ToolRunner;
import org.hackreduce.mappers.BixiMapper;
import org.hackreduce.mappers.ModelMapper;
import org.hackreduce.mappers.XMLInputFormat;
import org.hackreduce.mappers.XMLRecordReader;
import org.hackreduce.models.BixiRecord;

public class BixiStats extends org.hackreduce.examples.RecordCounter {

	public enum Count {
		TOTAL_RECORDS, UNIQUE_KEYS
	}

	public static class RecordCounterReducer extends
			Reducer<Text, LongWritable, Text, LongWritable> {

		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {
			context.getCounter(RecordCounterCount.UNIQUE_KEYS).increment(1);

			long count = 0;
			for (LongWritable value : values) {
				count += value.get();
			}

			context.write(key, new LongWritable(count));
		}

	}

	public static class RecordCounterMapper extends
			BixiMapper<Text, LongWritable> {

		// Our own made up key to send all counts to a single Reducer, so we can
		// aggregate a total value.
		public static final Text TOTAL_COUNT = new Text("total");

		// Just to save on object instantiation
		public static final LongWritable ONE_COUNT = new LongWritable(1);

		@Override
		protected void map(BixiRecord record, Context context)
				throws IOException, InterruptedException {

			context.getCounter(Count.TOTAL_RECORDS).increment(1);
			// context.write(TOTAL_COUNT, new
			// LongWritable(record.getNbBikes()));

			context.write(new Text(record.getRecordDateDay()+"_"+Integer.toString(record.getStationId())),
					      new LongWritable(record.getNbBikes()));
		}

	}

	@Override
	public void configureJob(Job job) {
		// The BIXI datasets are XML files with each station information
		// enclosed within
		// the <station></station> tags
		job.setInputFormatClass(XMLInputFormat.class);
		XMLRecordReader.setRecordTags(job, "<station>", "</station>");
	}

	@Override
	public Class<? extends ModelMapper<?, ?, ?, ?, ?>> getMapper() {
		return RecordCounterMapper.class;
	}

	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new BixiStats(),
				args);
		System.exit(result);
	}

}
