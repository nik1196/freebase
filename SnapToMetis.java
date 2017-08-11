import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;

/**
 * Created by jayanth on 7/28/2017.
 */

public class SnapToMetis {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Master");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = sc.textFile(args[0]);
        JavaPairRDD<Long, Long> vertexIdRDD = rdd.mapPartitions(new FlatMapFunction<Iterator<String>, Long>() {
            public Iterator<Long> call(Iterator<String> stringIterator) {
                HashSet<Long> vertexSet = new HashSet<Long>();
                while (stringIterator.hasNext()) {
                    String s = stringIterator.next();
                    String[] vertices = s.split("\\s+");
                    for (String vertex : vertices) {
                        vertexSet.add(Long.parseLong(vertex));
                    }
                }
                return vertexSet.iterator();
            }
        }).repartition(160).distinct().zipWithIndex().persist(StorageLevel.MEMORY_AND_DISK());

        JavaPairRDD<Long, Long> edgeRDD = rdd.mapPartitionsToPair(new PairFlatMapFunction<Iterator<String>, Long, Long>() {
            public Iterator<Tuple2<Long, Long>> call(Iterator<String> stringIterator) {
                ArrayList<Tuple2<Long, Long>> edges = new ArrayList<Tuple2<Long, Long>>();
                while (stringIterator.hasNext()) {
                    String s = stringIterator.next();
                    String[] vertices = s.split("\\s+");
                    if (!vertices[0].equals(vertices[1])) {
                        edges.add(new Tuple2<Long, Long>(Long.parseLong(vertices[0]), Long.parseLong(vertices[1])));
                        edges.add(new Tuple2<Long, Long>(Long.parseLong(vertices[1]), Long.parseLong(vertices[0])));
                    }
                }
                return edges.iterator();
            }
        }).partitionBy(new HashPartitioner(160));

        JavaPairRDD<Long, Long> metisedgelist = edgeRDD.join(vertexIdRDD).mapToPair(new PairFunction<Tuple2<Long,Tuple2<Long,Long>>, Long, Long>() {
            public Tuple2<Long, Long> call(Tuple2<Long, Tuple2<Long, Long>> longTuple2Tuple2) {
                return new Tuple2<Long, Long>(longTuple2Tuple2._2()._1(), longTuple2Tuple2._2()._2());
            }
        }).join(vertexIdRDD).mapToPair(new PairFunction<Tuple2<Long,Tuple2<Long,Long>>, Long, Long>() {
            public Tuple2<Long, Long> call(Tuple2<Long, Tuple2<Long, Long>> longTuple2Tuple2) {
                return new Tuple2<Long, Long>(longTuple2Tuple2._2()._2() + 1, longTuple2Tuple2._2()._1() + 1);
            }
        }).sortByKey();

        metisedgelist.map(new Function<Tuple2<Long, Long>, String>() {
            public String call(Tuple2<Long, Long> v1) throws Exception {
                StringBuilder sb = new StringBuilder(v1._1().toString() + " " + v1._2().toString());
                return sb.toString();
            }
        });//.saveAsTextFile(args[1]);

        vertexIdRDD.unpersist();

        metisedgelist.aggregateByKey(new ArrayList<Long>(), 160, new Function2<ArrayList<Long>, Long, ArrayList<Long>>() {
            public ArrayList<Long> call(ArrayList<Long> v1, Long v2) throws Exception {
                v1.add(v2);
                return v1;
            }
        }, new Function2<ArrayList<Long>, ArrayList<Long>, ArrayList<Long>>() {
            public ArrayList<Long> call(ArrayList<Long> v1, ArrayList<Long> v2) throws Exception {
                v1.addAll(v2);
                return v1;
            }
        }).sortByKey().map(new Function<Tuple2<Long,ArrayList<Long>>, String>() {
            public String call(Tuple2<Long, ArrayList<Long>> v1) throws Exception {
                StringBuilder sb = new StringBuilder(v1._1().toString() + " " + v1._2().size() + " ");
                for (Long l : v1._2()) {
                    sb.append(l.toString() + " ");
                }
                return sb.toString();
            }
        }).saveAsTextFile(args[2]);
    }
}
