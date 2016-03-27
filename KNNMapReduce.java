import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.util.Collections;
import java.util.stream.Collectors;
import java.io.BufferedReader;
import java.util.StringTokenizer;
import java.util.*;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;


//package KNNMapReduce;
public class KNNMapReduce {
	
	
    //class row to create instance for each row
    public static class RowData
    {	int age;
    	int income;
    	String marriage;
    	String gender;
    	String children;
    	
    	public RowData(){}
    	public RowData(String rowInput)
    	{
    		String[] features = rowInput.split("\\s*,\\s*");
    		this.age = Integer.parseInt(features[0]);
    		this.income = Integer.parseInt(features[1]);
    		this.marriage = features[2];
    		this.gender = features[3];
    		this.children = features[4];
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
    	public String getChildren(){
    		return children;
    	}
    }
    
	//overwrite IntWritable to write distance and label
    public static class ComputeDistance
    {
    	double trainAge, testAge;
    	double trainIncome, testIncome;
    	String trainMarriage, testMarriage;
    	String trainGender, testGender;
    	String trainChildren, testChildren;
    	String trainLabel;
    	double totalDistance;
    	public ComputeDistance(){}
    	public ComputeDistance(double testAge, double testIncome, String testMarriage, String testGender, String testChildren,
    							double trainAge, double trainIncome, String trainMarriage, String trainGender, String trainChildren)
    	{
    		this.testAge = testAge;
    		this.testIncome = testIncome;
    		this.testMarriage = testMarriage;
    		this.testGender = testGender;
    		this.testChildren = testChildren;
    		this.trainAge = trainAge;
    		this.trainIncome = trainIncome;
    		this.trainMarriage = trainMarriage;
    		this.trainGender = trainGender;
    		this.trainChildren = trainChildren;
    	}

    	public double pseudoEuclideanDist(double x1,double x2)
    	{
    		return Math.pow((x1-x2),2);
    	}
    	public double hammingDist(String a, String b)
    	{
    		if(a==b)
    		{
    			return 0;
    		}else
    		{
    			return 1;
    		}
    	}
    	public void computeDistance()
    	{
    		totalDistance = Math.sqrt(pseudoEuclideanDist(trainAge, testAge)+ pseudoEuclideanDist(trainIncome, testIncome))+
    						hammingDist(trainMarriage, testMarriage)+hammingDist(trainGender, testGender)+
    						hammingDist(trainChildren, testChildren);
    	}
    	public double getTotalDistance(){
    		return totalDistance;
    	}
    	
    }
    //class DistLabelWritable extend writable
    //http://hadooptutorial.info/creating-custom-hadoop-writable-data-type/
    
    
	//class KNNMapper for processing map step
	public static class KNNMapper extends Mapper<Object, Text, Text, Text>
	{	
		private Text distAndLabel,testFeature;	
		//variable test is to store the testing data
		//http://stackoverflow.com/questions/10416653/best-way-to-store-a-table-of-data
	    private ArrayList<RowData> test = new ArrayList<RowData>();
	    String strTest;
	    double trainAge, testAge;
	    double trainIncome, testIncome;
	    String trainMarriage, testMarriage;
	    String trainGender, testGender;
	    String trainChildren, testChildren;
	    String trainLabel, testLabel;
	    double totalDist;
	    
	    //ranges of continuous input data
	    double minAge = 18;
	    double maxAge = 77;
	    double minIncome = 50000;
	    double maxIncome = 67789;
	    
		//normalise continuous data
		public double scaling(double x, double min, double max){
			return (x-min)/(max-min);
		}
	    
		//the setup function is run once pre-processing data(get test set)
		public void setup(Context context)throws IOException
		{	
			//get file from context
			Configuration conf = context.getConfiguration();
			URI [] cacheFiles = context.getCacheFiles();
			String [] fn = cacheFiles[0].toString().split("#");
			String str;
			BufferedReader br = new BufferedReader(new FileReader(fn[1]));//localname??
			//RowData test = new RowData();
			//while(br!=null){
			//http://stackoverflow.com/questions/13405822/using-bufferedreader-readline-in-a-while-loop-properly
			for (String line = br.readLine(); line != null; line = br.readLine()) {
				//add data to data structure
				test.add(new RowData(line));
			}
			br.close();
		}

		//perform map step
		public void mapper(Object key, Text value, Context context)throws IOException, InterruptedIOException, InterruptedException
		{	
			String strDistAndLabel;	
			String rLine = value.toString();
			StringTokenizer tokens = new StringTokenizer(rLine, "\\s*,\\s*");
			trainAge = scaling(Double.parseDouble(tokens.nextToken()),minAge, maxAge);
			trainIncome = scaling(Double.parseDouble(tokens.nextToken()), minIncome, maxIncome);
			trainMarriage = tokens.nextToken();
			trainGender = tokens.nextToken();
			trainChildren = tokens.nextToken();
			trainLabel = tokens.nextToken();
			for(RowData t:test)
			{	testAge = scaling(t.getAge(),minAge, maxAge);
				testIncome = scaling(t.getIncome(), minIncome, maxIncome);
				testMarriage = t.getMarriage();
				testGender = t.getGender();
				testChildren = t.getChildren();
				ComputeDistance dist = new ComputeDistance(testAge, testIncome, testMarriage, testGender, testChildren,
															trainAge, trainIncome, trainMarriage, trainGender, trainChildren);
				totalDist = dist.getTotalDistance();
				strDistAndLabel = String.valueOf(totalDist)+","+trainLabel;
				distAndLabel = new Text();
				distAndLabel.set(strDistAndLabel);
				strTest = t.getAge()+"_"+t.getIncome()+"_"+t.getMarriage()+"_"+t.getGender()+"_"+t.getChildren();
				testFeature = new Text();
				testFeature.set(strTest);
				context.write(testFeature, distAndLabel);
			}
			
		}
	}
	//---------------------END MAP------------------------
	public static class KNNReducer extends Reducer<Text, Text, Text, Text>{
		
		
		Map<String, String> labelDistTuple = new HashMap<String, String>();
		private Text label = new Text();
		double value;
		
		//the setup function is run once pre-processing data(get test set)
		public void setup(Context context)throws IOException
		{	
			//get K from context
			Configuration conf =  context.getConfiguration();
			//int K = conf.getInt("K");
			String paramK = conf.get("paramK");
			int K = Integer.parseInt(paramK);

		}
		public void reducer(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException
		{	
			int count=0;
			int max=0;
			String classLabel="";
			for(Text t: values)
			{
				String rLine = t.toString();
				String[] keyvalue = rLine.split(","); 
				labelDistTuple.put(keyvalue[1],keyvalue[0]);
			}
			//sort HashMap : http://stackoverflow.com/questions/8119366/sorting-hashmap-by-values
			Map<String, String> sortedMap = 
					labelDistTuple.entrySet().stream()
				    .sorted(Entry.comparingByValue())
				    .collect(Collectors.toMap(Entry::getKey, Entry::getValue,
				                              (e1, e2) -> e1, LinkedHashMap::new));
			//get only labels to do a majority vote: http://stackoverflow.com/questions/1026723/how-to-convert-a-map-to-list-in-java
			List<String> labList = new ArrayList<String>(sortedMap.values());
			
			Set uniqueLabels = new HashSet(labList);
			//http://www.coderanch.com/t/381829/java/java/Casting-Object-String
			String[] tmpClass = (String[]) uniqueLabels.toArray(new String[uniqueLabels.size()]);
			for(String l: tmpClass){
				count = Collections.frequency(labList, l);
				if(count>max)
				{
					max=count;
					classLabel=l;
				}
			}

			label.set(classLabel);
			context.write(key, label);
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
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//sending parameters to MR
		//1. training set(path in HDFS)
		//2. path for result
		//3. test set
		//4. number of neighbours(k) for vote
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.addCacheFile(new URI(args[2]));//e.g. "/home/bwi/cache/file1.txt#first"
		//job.addCacheFile(new URI("/home/bwi/cache/test.txt#test"));
		//int k = Integer.parseInt(args[3]);
		//setInt("K", k); //the number of k-nearest
		String strK = args[3];
		conf.set("paramK",strK);
		//job.waitForCompletion(true);
		//Counters counter = job.getCounters();
		//System.out.println("Input Records: "+counters.findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue());
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
