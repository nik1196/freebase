
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;

import java.util.*;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import scala.Tuple2;

public class Driver {
    static JavaRDD<String> inputListRDD;

    public static JavaRDD<String> filter(Boolean byVertex){
        JavaRDD<String> filteredRDD;
        if (!byVertex){
            filteredRDD = inputListRDD.filter(new Function<String, Boolean>(){
                public Boolean call(String s){
                    if(s.split("\\s+")[2].startsWith("<"))
                        return true;
                    return false;
                }
            });
        }
        else{
            filteredRDD = inputListRDD.filter(new Function<String, Boolean>(){
                public Boolean call(String s){
                    if(s.split("\\s+")[2].startsWith("<"))
                        return false;
                    return true;
                }
            });
        }
        return filteredRDD;
    }


    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);
        String edgeFile = "/home/nikhil/repos/freebase/Freebase100.txt";
        SparkConf conf = new SparkConf().setAppName("Freebase");
        JavaSparkContext sc = new JavaSparkContext(conf);
        inputListRDD = sc.textFile(edgeFile).cache();

        JavaRDD<String> inputEdgesRDD = filter(false);
        JavaRDD<String> backEdgesRDD = inputEdgesRDD.map(new BackEdgesMapper());
        JavaPairRDD<String, String>completeEdgeList = inputEdgesRDD.union(backEdgesRDD).mapToPair(new EdgeListMapper());
        //partitioning experiment

        //JavaRDD<scala.Tuple2<String,String>> vidRDD = completeEdgeList.mapPartitionsWithIndex(new VidMapper(),false);

        JavaPairRDD<String,String> vidRDD = completeEdgeList.map(new InputSourceGetter()).distinct().mapToPair(new VidMapper2());

        JavaPairRDD<String,String> adjListLabel = completeEdgeList.reduceByKey(new AdjacencyListReducer());

        Iterator<scala.Tuple2<String, String>> adjListIterator = vidRDD.toLocalIterator();
        while(adjListIterator.hasNext())
            System.out.println(adjListIterator.next());

        System.out.println("adjList");

        Iterator<scala.Tuple2<String, String>> adjListIterator2 =adjListLabel.toLocalIterator();
        while(adjListIterator2.hasNext())
            System.out.println(adjListIterator2.next());
        // JavaPairRDD<String, Tuple2<String,String>> completeEdgeListVid = completeEdgeList.mapToPair(new AdjListWithVidMapper(vidRDD));
        /*JavaPairRDD<String,String> adjListRDD = inputEdgesRDD.mapToPair(new AdjacencyListMapper());
        JavaPairRDD<String,String> edgeListRDD = inputEdgesRDD.mapToPair(new EdgeListMapper(vids)).reduceByKey(new EdgeListReducer());*/

        JavaRDD<scala.Tuple3<String,String,String>> vertexListRDD = filter(true).map(new VertexListMapper());

        //adjListRDD = inputEdgesRDD.mapToPair(new InputLabelsToVidMapper(vids)).reduceByKey(new InputLabelsToVidReducer());

    }
}
