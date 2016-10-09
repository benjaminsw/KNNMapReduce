# KNN MapReduce

##k-nearest neighbors algorithm
 The k-Nearest Neighbors algorithm (or kNN for short) is a non-parametric method used for classification problem.
 
##MapReduce
MapReduce is a framework for processing large dataset that distributes across multiple computer (nodes). The framework consists of Map() function and Reduce() function, primarily. 

###Mapper
The map function takes any input files as its input and generate a key/value pairs as its output. 
###Reducer
The reduce function take key/values pairs that emit from the map function as its input and combile the value of the input that has the same key and finally emit the result.

##KNN MapReduce
In this exercise, I have implementd kNN MapReduce which tried to predict what type of care people in the testset may own. The map function takes a CSV file as its input. The attributes in the input file consists of "age", "salary", "married status", "gender", "the number of children" and "car type" as well as the test set. I represents each row in object format. Then I compute the distance of traingset against testset and emitted testset objects as key and the distance as value.

Now, the reduce function takes these key/value pairs as its input and then sort the distance in ascending order. The reducer also picks up how many neighbour we want to use for our clssification as well. After sorting, the reducer selects top k value and performs the majority vote in order to predict what type of car that people in our test set may own.

##MapReduce job submission
*parameter 1: directory where data resides 
*parameter 2: directory where to put result 
*parameter 3: directory for the test data concatenating with variable assigning
to that file 
*parameter 4: number of nearest neighbour K 
and now we are ready to run MapReduce
```
hadoop jar KNNMapReduce.jar KNNMapReduce /home/usrname/knn
/home/usrname/res /home/usrname/cache/test.txt#test 5 
```
##Result inspection
Use command to view the result
```
hdfs dfs –cat /home/usrname/res/part-r-00000 
```

