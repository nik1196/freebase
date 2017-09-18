import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;

import java.util.*;
import org.apache.spark.api.java.Optional;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.spark.storage.StorageLevel;

public class Driver {
    public static JavaRDD<String> filter(JavaRDD<String> inputListRDD, Boolean byVertex){
        JavaRDD<String> filteredRDD;
        if (!byVertex){
            filteredRDD = inputListRDD.filter(new Function<String, Boolean>(){
                public Boolean call(String s){
                    String [] parts = s.split("\\s+");
                    if(parts[2].startsWith("<")){
                        for(int i=3;i<parts.length;i++){
                            if(!parts[i].equals("."))
                                parts[2] = parts[2].concat(parts[i]);
                        }
                        if(!parts[0].equals(parts[2]))
                            return true;
                    }
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

    public static void displayRDD(JavaPairRDD iterable, String message){
        if(!message.equals(""))
            System.out.println(message);
        Iterator iterator = iterable.toLocalIterator();
        while(iterator.hasNext())
            System.out.println(iterator.next());
        System.out.println("\n\n");
    }
    public static void displayRDD(JavaRDD iterable, String message){
        if(!message.equals(""))
            System.out.println(message);
        Iterator iterator = iterable.toLocalIterator();
        while(iterator.hasNext())
            System.out.println(iterator.next());
        System.out.println("\n\n");
    }
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);
        String edgeFile = "/home/nikhil/repos/freebase/Freebase100.txt";//"nishank/sampleinput/freebase-rdf-latest.gz";
        SparkConf conf = new SparkConf().setAppName("Freebase");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext sc = new JavaSparkContext(conf);
        int total_cores = 2;// Integer.parseInt(conf.get("spark.executor.instances")) * Integer.parseInt(conf.get("spark.executor.cores"));
        JavaRDD<String> inputListRDD = sc.textFile(edgeFile).repartition(total_cores*24);

        JavaPairRDD<String,String> inputEdgesRDD = filter(inputListRDD,false).mapToPair(new EListMapper()).partitionBy(new HashPartitioner(total_cores*24)).persist(StorageLevel.DISK_ONLY());



        JavaPairRDD<String,String> vertexLabels = inputEdgesRDD.flatMapToPair(new InputVerticesGetter()).distinct();


        JavaPairRDD<String,Long> vidRDD = vertexLabels.map(new InputSourceGetter()).distinct().zipWithIndex().mapToPair(new VidMapper2()).persist(StorageLevel.DISK_ONLY());
        //vidRDD.saveAsTextFile("output/vidMapping");
        /*JavaPairRDD<Long, String> adjListVid = */vertexLabels.join(vidRDD).mapToPair(new JoinedRDDToPairRDDMapper()).join(vidRDD).mapToPair(new JoinedRDDToPairRDDMapper2()).reduceByKey(new AdjacencyListReducer()).sortByKey().saveAsTextFile("output/freebaseMetis");
        //JavaPairRDD<String,String> adjListLabel = completeEdgeList.reduceByKey(new AdjacencyListReducer());

        JavaPairRDD<String,String> srcPropRDD = filter(inputListRDD,true).mapToPair(new VertexListMapper()).reduceByKey(new VertexListReducer()).mapToPair(new VertexPropertyNumberAssigner());

        JavaPairRDD<String, String>completeEdgeList = inputEdgesRDD.flatMapToPair(new BackEdgesMapper());

        //displayRDD(completeEdgeList, "complete edge list");
        JavaPairRDD<String, scala.Tuple2<Long,Optional<String>>> srcLabel_srcId_srcProp= vidRDD.leftOuterJoin(srcPropRDD);
        //srcLabel_srcId_srcProp.map(new AltFormatMapper()).saveAsTextFile("output/altFormat");

        //displayRDD(srcLabel_srcId_srcProp, "source prop");

        JavaPairRDD<scala.Tuple2<String,String>,String > srcLabelSnkLabel_Eprops = completeEdgeList.mapToPair(new SinkGetter()).join(vidRDD).mapToPair(new SinkLabelReformatterMapper()).mapToPair(new EpropAsValue_Mapper()).reduceByKey(new EpropAsValue_Reducer());

        //displayRDD(srcLabelSnkLabel_Eprops, "srcsnkep");

        //vidRDD.unpersist();
        //inputEdgesRDD.unpersist();

        JavaPairRDD<String, String> srcLabel_SnkLabelEprops = srcLabelSnkLabel_Eprops.mapToPair(new EPropNumberAssignerMapper()).mapToPair(new Snk_Eprop_AsValue_Mapper()).reduceByKey(new Snk_Eprop_AsValue_Reducer());

        //displayRDD(srcLabel_SnkLabelEprops, "reduced");

        JavaPairRDD<Long, scala.Tuple2<String, String>> finalJsonFormat = srcLabel_srcId_srcProp.join(srcLabel_SnkLabelEprops).mapToPair(new Label_Id_Swap_Mapper()).sortByKey();
        finalJsonFormat.saveAsTextFile("output/freebaseJsonFormat");



        displayRDD(finalJsonFormat, "final");

        //displayRDD(completeEdgeList, "completeEdgeList");
        //displayRDD(vidRDD, "vidRDD");
        //displayRDD(slbl_dlbl_svid, "slbl_dlbl_svid");
        //displayRDD(dlbl_svid, "dlbl_svid");
        //displayRDD(adjListVid, "adjListVid");
        //displayRDD(finalJsonFormat, "finalJsonFormat");


    }
}
