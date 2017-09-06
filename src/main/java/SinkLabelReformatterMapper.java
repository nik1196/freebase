import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class SinkLabelReformatterMapper implements PairFunction<Tuple2<String, Tuple2<String, Long>>, String, String> {
    public scala.Tuple2<String,String> call(scala.Tuple2<String, scala.Tuple2<String,Long>> s){
        String [] parts = s._2()._1().split("\\s+");
        return new scala.Tuple2<String, String>(parts[0], parts[1] + " " + s._2()._2());
    }
}
