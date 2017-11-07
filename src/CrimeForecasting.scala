package org.spark.sparkshell1
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import com.cloudera.sparkts.models.ARIMA
import breeze.linalg._
import breeze.plot._


object projectCluster{
  def main(args: Array[String]) {
    val sc=new SparkContext("local[*]","KMeans Clustering")
    val data = sc.textFile("/Users/deepaks/Desktop/NIJ_CFS_PORTLAND/mod_NIJ_CFS_PORTLAND.csv")
    

    val splitData = data.map(line=> line.split(","))
    val parsedData = splitData.map(line=>Vectors.dense(line(0).toDouble,line(1).toDouble)).cache()
    val numClusters = 3
    val numIterations = 50
    val clusters = KMeans.train(parsedData, numClusters, numIterations)
    val centers = clusters.clusterCenters
  
    val formatter = new java.text.SimpleDateFormat("MM/dd/yyyy")
    
    val prediction = splitData.map(v => (v(0),v(1),v(2),v(3).toInt,clusters.predict(Vectors.dense(v(0).toDouble,v(1).toDouble))))

    val count0 = prediction.filter(_._5==0).count()
    val count1 = prediction.filter(_._5==1).count()
    val count2 = prediction.filter(_._5==2).count()
    println("Count of Cluster0: "+count0)
    println("Count of Cluster1: "+count1)
    println("Count of Cluster2: "+count2)
    
   
    val DF0 = prediction.filter(_._5==0).map(line=>(line._3,(line._1,line._2,line._4,line._5)))
    val DF1 = prediction.filter(_._5==1).map(line=>(line._3,(line._1,line._2,line._4,line._5)))
    val DF2 = prediction.filter(_._5==2).map(line=>(line._3,(line._1,line._2,line._4,line._5)))
    val AVG0 = DF0.groupByKey().map(data => { val tot = data._2.map({case (a,b,c,d)=>c}).sum; 
                                  (formatter.parse(data._1),tot.toDouble)}).sortByKey(true,1)
    val AVG1 = DF1.groupByKey().map(data => { val tot = data._2.map({case (a,b,c,d)=>c}).sum; 
                                  (formatter.parse(data._1),tot.toDouble)}).sortByKey(true,1)
    val AVG2 = DF2.groupByKey().map(data => { val tot = data._2.map({case (a,b,c,d)=>c}).sum; 
                                  (formatter.parse(data._1),tot.toDouble)}).sortByKey(true,1)
       
// Cluster 0 
    val ts0 = DenseVector(AVG0.map(_._2).toArray)
    val arimaModel0 = ARIMA.fitModel(1, 0, 1, ts0)
    val forecast0 = arimaModel0.forecast(ts0, 30)
    val result0 = forecast0.toArray
    val len0 = result0.length
    val arr1_1 = new Array[Double](len0-30)
    for(a<-0 to (len0-31))
    {
      arr1_1(a)=result0(a)
   
    }
    val dateVector0 = AVG0.map(_._1).toArray
    val graphData1_1 = sc.parallelize(dateVector0 zip (arr1_1)).sortByKey(true,1).saveAsTextFile("/Users/deepaks/Desktop/Output1_1/")
    
    val arr1_2 = new Array[String](31)
    var i=0
    for(a<-(len0-31) to (len0-1))
    {
      arr1_2(i)=result0(a).toString()
      i+=1

    }
   sc.parallelize(arr1_2,1).saveAsTextFile("/Users/deepaks/Desktop/Output1_2/")
   
  // Cluster 1
    val ts1 = DenseVector(AVG1.map(_._2).toArray)
    val arimaModel1 = ARIMA.fitModel(1, 0, 1, ts1)
    val forecast1 = arimaModel1.forecast(ts1, 30)
    val result1 = forecast1.toArray
    val len1 = result1.length
    val arr2_1 = new Array[Double](len1-30)
    for(a<-0 to (len1-31))
    {
      arr2_1(a)=result1(a)
   
    }
    val dateVector1 = AVG1.map(_._1).toArray
    val graphData2_1 = sc.parallelize(dateVector1 zip (arr2_1)).sortByKey(true,1).saveAsTextFile("/Users/deepaks/Desktop/Output2_1/")
    
    val arr2_2 = new Array[String](31)
    var j=0
    for(a<-(len1-31) to (len1-1))
    {
      arr2_2(j)=result1(a).toString()
      j+=1

    }
   sc.parallelize(arr2_2,1).saveAsTextFile("/Users/deepaks/Desktop/Output2_2/")
   
   
   
   // Cluster 2
    val ts2 = DenseVector(AVG2.map(_._2).toArray)
    val arimaModel2 = ARIMA.fitModel(1, 0, 1, ts2)
    val forecast2 = arimaModel2.forecast(ts2, 30)
    val result2 = forecast2.toArray
    val len2 = result2.length
    val arr3_1 = new Array[Double](len2-30)
    for(a<-0 to (len2-31))
    {
      arr3_1(a)=result2(a)
   
    }
    val dateVector2 = AVG2.map(_._1).toArray
    val graphData3_1 = sc.parallelize(dateVector2 zip (arr3_1)).sortByKey(true,1).saveAsTextFile("/Users/deepaks/Desktop/Output3_1/")
    
    val arr3_2 = new Array[String](31)
    var k=0
    for(a<-(len2-31) to (len2-1))
    {
      arr3_2(k)=result2(a).toString()
      k+=1

    }
   sc.parallelize(arr3_2,1).saveAsTextFile("/Users/deepaks/Desktop/Output3_2/")   
}
}