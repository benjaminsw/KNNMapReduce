import java.io.BufferedReader;
import java.util.StringTokenizer;

import javax.security.auth.login.Configuration;
import javax.xml.soap.Text;

//package KNNMapReduce;

public class KNNMapReduce {
	
	
    //class row to create instance for each row
    public static class RowData
    {	int age;
    	int income;
    	String marriage;
    	String gender;
    	int children;
    	
    	public RowData(){}
    	public RowData(String rowInput)
    	{
    		String[] features = rowInput.split(" ");
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
    	String trainMarriage, testMarrige;
    	String trainGender, testGender;
    	double trainChildren, testChildren;
    	String trainLabel;
    	double totalDistance;
    	public ComputeDistance(){}
    	public ComputeDistance(double testAge, double testIncome, String testMarriage, String testGender, String testChildren,
    							double trainAge, double trainIncome, String trainMarriage, String trainGender, String trainChildren)
    	{
    		this.testAge = testAge;
    		this.testIncome = testIncome;
    		this.testMarrige = testMarriage;
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
    	public double hammingDist(String a, Stirng b)
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
	public static class KNNMapper extends Mapper<Object, Text, Text, IntWritable>
	{	
		private Text distAndLabel;	
		//variable test is to store the testing data
		//http://stackoverflow.com/questions/10416653/best-way-to-store-a-table-of-data
	    private ArrayList<RowData> test = new ArrayList<RowData>();
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
	    
		//normalise continous data
		public double scaling(double x, double min, double max){
			return (x-min)/(max-min);
		}
	    
		//the setup function is run once pre-processing data(get test set)
		public void setup(Context context)throws IOException
		{	
			//get file from context
			Configuration conf = context.getConfiguaration();
			URI [] cacheFiles = context.getCacheFiles();
			String [] fn = cacheFiles[0].toString().split('#');
			String str;
			BufferedReader br = new BufferedRedaer(new FileReader(fn[1]));//localname??
			str = br.readLine();
			//RowData test = new RowData();
			while(br!=null){
				//add data to data structure
				test.add(new RowData(str));
				str = br.readLine();
			}
			br.close();
		}

		//perform map step
		public void mapper(Object key, Text value, Context context)throws IOException, InteruptedException
		{	
				
			String rLine = value.toString();
			StringTokenizer tokens = new StringTokenizer(rLine, ",");
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
				strDistAndLabel = totalDist.toString()+","+trainLabel;
				distAndLabel = new Text();
				distAndLabel.set(strDistAndLabel);
				context.write(t.toString(), distAndLabel);
			}
			
		}
	}
	//---------------------END MAP------------------------
	public static class KNNReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		
		Map<String, Integer> labelDistTuple = new HashMap<String, Integer>();
		private Text label = new Text();
		String key;
		double value;
		
		//the setup function is run once pre-processing data(get test set)
		public void setup(Context context)throws IOException
		{	
			//get K from context
			Configuration conf =  context.getConfiguration();
			Int K = conf.getInt("K");

		}
		public void reducer(Text key, Iterable<Text> values, Context cotext)throws IOException, InteruptedException
		{
			for(Text t: values)
			{
				String[] keyvalue = values.split(","); 
				labelDistTuple.put(Integer.parseInt(keyvalue[1]),keyvalue[0]);
			}
			//sort HashMap : http://stackoverflow.com/questions/8119366/sorting-hashmap-by-values
			Map<Integer, String> sortedMap = 
					labelDistTuple.entrySet().stream()
				    .sorted(Entry.comparingByValue())
				    .collect(Collectors.toMap(Entry::getKey, Entry::getValue,
				                              (e1, e2) -> e1, LinkedHashMap::new));
			//get only labels to do a majority vote: http://stackoverflow.com/questions/1026723/how-to-convert-a-map-to-list-in-java
			List<String> labList = new ArrayList<String>(sortedMap.values());
			//get most elements occued: http://stackoverflow.com/questions/19031213/java-get-most-common-element-in-a-list
			Integer maxOccurredElement = labList.stream()
			        .reduce(BinaryOperator.maxBy((o1, o2) -> Collections.frequency(labList, o1) -
			                        Collections.frequency(labList, o2))).orElse(null);
			label.set(maxOccurredElement);
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
		int k = Integer.parseInt(args[3]);
		conf.setInt("K", k); //the number of k-nearest 
		job.waitForCompleion(true);
		//Counters counter = job.getCounters();
		//System.out.println("Input Records: "+counters.findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue());
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
