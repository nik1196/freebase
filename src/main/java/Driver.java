import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;

import java.util.*;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

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
        JavaRDD<scala.Tuple3<String,String,String>>inputEdgesRDDWithBackEdges = inputEdgesRDD.union(backEdgesRDD).map(new EdgeListMapper());

        /*Iterator<scala.Tuple3<String,String,String>> iterator_inputEdgesRDDWithBackEdges = inputEdgesRDDWithBackEdges.toLocalIterator();
        while(iterator_inputEdgesRDDWithBackEdges.hasNext())
            System.out.println(iterator_inputEdgesRDDWithBackEdges.next());*/


        //partitioning experiment

        //JavaRDD<scala.Tuple2<String,String>> vidRDD = inputEdgesRDDWithBackEdges.mapPartitionsWithIndex(new VidMapper(),false);

        JavaRDD<scala.Tuple2<String,String>> vidRDD = inputEdgesRDDWithBackEdges.map(new InputSourceGetter()).distinct().map(new VidMapper2());
        Iterator<scala.Tuple2<String,String>> iterator_vidRDD = vidRDD.toLocalIterator();
        while(iterator_vidRDD.hasNext())
            System.out.println(iterator_vidRDD.next());
        //

        /*JavaPairRDD<String,String> adjListRDD = inputEdgesRDD.mapToPair(new AdjacencyListMapper());

        JavaPairRDD<String,String> edgeListRDD = inputEdgesRDD.mapToPair(new EdgeListMapper(vids)).reduceByKey(new EdgeListReducer());*/

        JavaRDD<scala.Tuple3<String,String,String>> vertexListRDD = filter(true).map(new VertexListMapper());

        Iterator<scala.Tuple3<String,String,String>> iterator_vertexListRDD = vertexListRDD.toLocalIterator();
        /*while(iterator_vertexListRDD.hasNext())
            System.out.println(iterator_vertexListRDD.next());/*



        


        //adjListRDD = inputEdgesRDD.mapToPair(new InputLabelsToVidMapper(vids)).reduceByKey(new InputLabelsToVidReducer());


        /*Iterator<scala.Tuple2<String,String>> iterator_adjListRDD = adjListRDD.toLocalIterator();
        while(iterator_adjListRDD.hasNext()) {
            System.out.println(iterator_adjListRDD.next());
        }*/
    }
}
