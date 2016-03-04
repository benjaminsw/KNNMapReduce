import javax.xml.soap.Text;

//package KNNMapReduce;

public class KNNMapReduce {
	/*
	 * class KNNMapper for processing map step
	 * -receive 3 arguments
	 * 		1. training set(path in HDFS)
	 * 		2. path for result
	 * 		3. test set
	 * 		4. number of neighbours(k) for vote
	*/
	public static class KNNMapper extends Mapper<Object, Text, Text, IntWritable>
	{	
		//pre-processing data
		//	-get test set
		// 	-get k
		@override
		public void setup(Context context)throws IOException, InterutedException
		{
			//implement
		}
		//perform map step
		public void map(Object key, Text value, Context context)throws IOException, InteruptedException
		{
			//implement
		}
	}
	//---------------------END MAP------------------------
	public static class KNNReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		public void reduce(Text key, Iterable<IntWritable> values, Context cotext)throws IOException, InteruptedException
		{
			//implement
		}
	}
	//--------------------END REDUCE----------------------
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "k-nearest neighbour");
		job.setJarByClass(KNNMapReduce.class);
		job.setMapperClass(KNNMapper.class);
		job.setCombinerClass(KNNReducer.class);
		job.setReducerClass(KNNReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompleion(true);
		Counters counter = job.getCounters();
		System.out.println("Input Records: "+counters.findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue());
	}
}
