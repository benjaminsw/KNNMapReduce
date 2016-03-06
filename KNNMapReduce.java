import java.io.BufferedReader;

import javax.xml.soap.Text;

//package KNNMapReduce;

public class KNNMapReduce {
	
	//variable test is to store the testing data
	//http://stackoverflow.com/questions/10416653/best-way-to-store-a-table-of-data
    private ArrayList<rowData> test = new ArrayList<rowData>();
    
    //class row to create instance for each row
    public static class rowData
    {	int age;
    	int income;
    	String marriage;
    	String gender;
    	int children;
    	
    	public void rowData(){}
    	public void rowData(String rowInput)
    	{
    		String[] features = rowInput.split(" ");
    		this.age = Integer.parseInt(features[0]);
    		this.income = Integer.parseInt(features[1]);
    		this.marriage = features[2];
    		this.gender = features[3];
    		this.children = Integer.parseInt(features[4]);
    	}
    	public int getAge(){
    		return age;
    	}
    	public int getIncome(){
    		return income;
    	}
    	public String getMarriage(){
    		return marriage;
    	}
    	public String getGender(){
    		return gender;
    	}
    	public int getChildren(){
    		return children;
    	}
    }
	
	//class KNNMapper for processing map step
	public static class KNNMapper extends Mapper<Object, Text, Text, IntWritable>
	{	
		//the setup function is run once pre-processing data(get test set)
		public void setup(Context context)throws IOException
		{	
			//get file from context
			Configuration conf = context.getConfiguaration();
			URI[] cacheFiles = context.getCacheFiles();
			String [] fn = cacheFiles[0].toString().split('#');
			BufferedReader br = new BufferedRedaer(new FileReader(fn[1]));//localname??
			//int count = 0//keep records of number of line readin so far
			while(br!=null){
				test.add(new rowData(br));
				//add data to data structure
				//count++;
			}
			br.close();
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
		//sending parameters to MR
		//1. training set(path in HDFS)
		//2. path for result
		//3. test set
		//4. number of neighbours(k) for vote
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.addCacheFile(new URI(args[2]));//e.g. "/home/bwi/cache/file1.txt#first"
		conf.set("K", args[3]); //the number of k-nearest 
		job.waitForCompleion(true);
		Counters counter = job.getCounters();
		System.out.println("Input Records: "+counters.findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue());
	}
}
