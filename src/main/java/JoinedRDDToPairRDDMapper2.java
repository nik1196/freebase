import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class JoinedRDDToPairRDDMapper2 implements PairFunction<Tuple2<String, Tuple2<String,Long>>, Long, String>{
public Tuple2<Long, String> call(Tuple2<String, Tuple2<String,Long>> s){

        return new Tuple2<Long, String>(Long.parseLong(s._2._1()), s._2()._2().toString());
    }
}
