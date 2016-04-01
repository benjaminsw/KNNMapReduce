import java.io.IOException;
import java.io.InterruptedIOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.util.stream.Collectors;
import java.io.BufferedReader;
import java.util.*;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
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
    		totalDistance = Math.sqrt(pseudoEuclideanDist(trainAge, testAge)+ pseudoEuclideanDist(trainIncome, testIncome))+
					hammingDist(trainMarriage, testMarriage)+hammingDist(trainGender, testGender)+
					hammingDist(trainChildren, testChildren);
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
    	
    	public double getTotalDistance(){
    		return totalDistance;
    		
    	}
    	
    }
    //http://beginnersbook.com/2013/12/java-arraylist-of-object-sort-example-comparable-and-comparator/
    public static class DistNTest implements Comparable<DistNTest>{
    	private double dist;
    	private RowData t;
    	public DistNTest(){}
    	public DistNTest(double dist, RowData t){
    		this.dist = dist;
    		this.t= t;
    	}
    	public RowData getT(){
    		return t;
    	}
    	public double getDist(){
    		return dist;
    	}
    	//http://www.tutorialspoint.com/java/number_compareto.htm
    	@Override
    	public int compareTo(DistNTest compareDist) {
            double compareDistance=((DistNTest)compareDist).getDist();
            /* For Ascending order*/
            if(this.dist==compareDistance){
            	return 0;
            }else if (this.dist>compareDistance){
            	return 1;
            }//else if (this.dist<compareDistance){
            	//return -1;
            //}
            return -1;
    	}
    }
        
	//class KNNMapper for processing map step
	public static class KNNMapper extends Mapper<Object, Text, Text, Text>
	{	
		private RowData rowData;
		private DistNTest tmpDistNTest;
		private Text distAndLabel,testFeature;	
		//variable test is to store the testing data
		//http://stackoverflow.com/questions/10416653/best-way-to-store-a-table-of-data
	    private ArrayList<RowData> test = new ArrayList<RowData>();
	    private ArrayList<DistNTest> distArray = new ArrayList<DistNTest>();
	    private String strTest;
	    private double trainAge, testAge;
	    private double trainIncome, testIncome;
	    private String trainMarriage, testMarriage;
	    private String trainGender, testGender;
	    private String trainChildren, testChildren;
	    private String trainLabel;
	    private double totalDist;
	    private int K;
	    private String strDistAndLabel;
	    
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
			
			BufferedReader br = new BufferedReader(new FileReader(fn[1]));//localname??
			String paramK = conf.get("paramK");
			K = Integer.parseInt(paramK);
			//http://stackoverflow.com/questions/13405822/using-bufferedreader-readline-in-a-while-loop-properly
			for (String line = br.readLine(); line != null; line = br.readLine()) {
				//add data to data structure
				test.add(new RowData(line));
			}
			br.close();
		}

		//perform map step
		public void map(Object key, Text value, Context context)throws IOException, InterruptedIOException, InterruptedException
		{		
			String rLine = value.toString();
			String[] content=rLine.split(",");
			trainAge = scaling(Double.parseDouble(content[0]),minAge, maxAge);
			trainIncome = scaling(Double.parseDouble(content[1]), minIncome, maxIncome);
			trainMarriage = content[2];
			trainGender = content[3];
			trainChildren =content[4];
			trainLabel = content[5];
			for(RowData t:test)
			{	testAge = scaling(t.getAge(),minAge, maxAge);
				testIncome = scaling(t.getIncome(), minIncome, maxIncome);
				testMarriage = t.getMarriage();
				testGender = t.getGender();
				testChildren = t.getChildren();
				ComputeDistance dist = new ComputeDistance(testAge, testIncome, testMarriage, testGender, testChildren,
															trainAge, trainIncome, trainMarriage, trainGender, trainChildren);
				totalDist = dist.getTotalDistance();
				distArray.add(new DistNTest(totalDist,t));
			}
			Collections.sort(distArray);
			for(int i=0; i<K; i++){
				tmpDistNTest = distArray.get(i);
				totalDist = tmpDistNTest.getDist();
				rowData = tmpDistNTest.getT();
				strDistAndLabel = totalDist+","+trainLabel;
				distAndLabel = new Text();
				distAndLabel.set(strDistAndLabel);
				strTest = rowData.getAge()+"_"+rowData.getIncome()+"_"+rowData.getMarriage()+"_"+rowData.getGender()+"_"+rowData.getChildren();
				testFeature = new Text();
				testFeature.set(strTest);
				context.write(testFeature, distAndLabel);
				
			}
		}
	}
	//---------------------END MAP------------------------
	public static class DistLabelTuple
	{
	    private final String key;
	    private final Double value;

	    public DistLabelTuple(String value, String key)
	    {	
	    	this.value = Double.parseDouble(value);
	        this.key   = key; 
	    }

	    public String getLabel()   { return key; }
	    public Double getDist() { return value; }
	    
	    @Override
	    public boolean equals(Object arg0) {
	        boolean flag = false;
	        DistLabelTuple kv = (DistLabelTuple) arg0;
	        if(null!= kv && kv.getLabel().equalsIgnoreCase(key) ){
	            flag = true;
	        }
	        return flag;
	    }
	}
	public static class DistComparator implements Comparator<DistLabelTuple> {
		@Override
		public int compare(DistLabelTuple o1, DistLabelTuple o2) {
		    if (o1.getDist() > o2.getDist()) {
		        return 1;
		    } else if (o1.getDist() < o2.getDist()) {
		        return -1;
		    }
		    return 0;
		}}
	
	public static class KNNReducer extends Reducer<Text, Text, Text, Text>{
		 
		private ArrayList<DistLabelTuple> distNLabel = new ArrayList<DistLabelTuple>();
		//private ArrayList<DistLabelTuple> kNeighbour;
		private Text label = new Text();
		private List<DistLabelTuple> kNeighbour ;
		//the setup function is run once pre-processing data(get test set)
		public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException
		{	
			int count=0;
			int max=-1;
			String classLabel="";
			for(Text t: values)
			{
				String rLine = t.toString();
				String[] keyvalue = rLine.split(","); 
				distNLabel.add(new DistLabelTuple(keyvalue[0],keyvalue[1]));
				
			}
			//http://stackoverflow.com/questions/14475556/how-to-sort-arraylist-of-objects
			Collections.sort(distNLabel, new DistComparator());
			kNeighbour = new ArrayList<>(distNLabel);
			Set<DistLabelTuple> uniqueNeigbour = new HashSet<DistLabelTuple>(kNeighbour);
	        for (DistLabelTuple uniqN : uniqueNeigbour) {
	            count= Collections.frequency(kNeighbour, uniqN);
	            if(count>max){
	            	max = count;
	            	classLabel = uniqN.getLabel();
	            }
	        }
			label.set(classLabel);
			context.write(key, label);
		}
		
	}
	//--------------------END REDUCE----------------------
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		String strK = args[3];
		conf.set("paramK",strK);
		Job job = Job.getInstance(conf, "k-nearest neighbour");
		job.setJarByClass(KNNMapReduce.class);
		job.setMapperClass(KNNMapper.class);
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
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
