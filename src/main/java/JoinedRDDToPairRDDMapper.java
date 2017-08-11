import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class JoinedRDDToPairRDDMapper implements PairFunction<Tuple2<String, Tuple2<String,String>>, String, String>{
public scala.Tuple2<String,String> call(scala.Tuple2<String, scala.Tuple2<String,String>> s){
        return new scala.Tuple2<String,String>(s._2()._1(), s._2()._2());
    }
}
