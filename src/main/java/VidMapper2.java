import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class VidMapper2 implements Function<scala.Tuple2<String,Long>, scala.Tuple2<String,Long>>{
    public scala.Tuple2<String,Long> call(scala.Tuple2<String,Long> s){
        return new scala.Tuple2<String,Long>(s._1(), s._2() + 1);
    }
}
