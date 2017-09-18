import org.apache.spark.api.java.function.PairFunction;

public class BackEdgesMapper2 implements PairFunction<scala.Tuple2<Long,String>, Long,String>{
    public scala.Tuple2<Long,String> call(scala.Tuple2<Long,String> s){
        return new scala.Tuple2<Long, String>(Long.parseLong(s._2()), String.valueOf(s._1()));
    }
}
