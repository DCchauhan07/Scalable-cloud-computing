-----------------------------------Readme file---------------------------


Make sure required installation part is done. Pyspark and Mapreduce is installed with required libraries.
--Apache Spark (updated version)
- Python (updated version)
- Java
--MRJob

BGL Log Analysis with Apache Spark :
	1)For Pyspark implementation Run the command: pyspark
	1)Place your log file named "BGL.log" in the same directory as this script and in jupyter notebook as well.


BGL Log Analysis with Hadoop MapReduce Pattern :
	1)Setup for MapReduce- install the MRJob library
	1)Start the implementation of code, after saving the .py in your path of terminal makes sure you are giving 'chmod +x 14.py' execution permission for the file.
	2)Run below command to get the output :
		time python 14.py -r hadoop hdfs:///user/hduser/BGL.log
