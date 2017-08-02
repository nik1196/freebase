import org.apache.spark.api.java.function.Function;

public class VidMapper2 implements Function<String, scala.Tuple2<String,String>>{
    static long vid=1;
    public scala.Tuple2<String,String> call(String s){
               return new scala.Tuple2<String,String>(s, String.valueOf(vid++));
        }
}
