import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class Snk_Eprop_AsValue_Mapper implements PairFunction<Tuple2<Tuple2<String,String>, String>,String,String> {
    public scala.Tuple2<String,String> call (Tuple2<Tuple2<String,String>, String> s){
        return new scala.Tuple2<String,String> (s._1()._1(), s._1()._2() + s._2());
    }
}
