import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer token_itr = new StringTokenizer(value.toString());
			
			/* storing file name */
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String filename = fileSplit.getPath().getName();
			
			/* looping until all the words in a line of a file are processed */
			while (token_itr.hasMoreTokens()) {
					String token = token_itr.nextToken();
					String filter_token = token.replaceAll("[^a-zA-Z]", "");
					String lower_token = filter_token.toLowerCase();
				
                	word.set(lower_token);
		 		
				
					context.write(word, new Text(filename));
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			ArrayList<String> fileArray = new ArrayList<String>();
			
			for (Text val : values) {
				if (!fileArray.contains(val.toString()))
					fileArray.add(val.toString());
			}
			
			/* getting the occurrence of a word (key) in different files by taking size of the stores file name values*/
			context.write(key, new Text(fileArray.size() + ""));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);

		// specify the output type for both map and reduce
		job.setOutputKeyClass(Text.class);
		
		job.setOutputValueClass(Text.class);

	    //if the output type of map task is different from reduce task,specify output type as follows

	    job.setMapOutputKeyClass(Text.class);

	    job.setMapOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

