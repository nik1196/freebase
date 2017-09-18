import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class EpropAsValue_Mapper implements PairFunction<Tuple2<String,String>, scala.Tuple2<String,String>, String> {
    public scala.Tuple2<scala.Tuple2<String,String>,String> call(scala.Tuple2<String,String> s){
        String [] parts = s._2().split("\\s+");
        return new scala.Tuple2<scala.Tuple2<String,String>, String>(new scala.Tuple2(s._1(), parts[1]), parts[0]);
    }
}
