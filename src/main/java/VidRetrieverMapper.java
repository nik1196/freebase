import org.apache.spark.api.java.function.PairFunction;

public class VidRetrieverMapper implements PairFunction<String,String,Long>{
    public scala.Tuple2<String,Long> call(String s){
        String [] parts = s.substring(1,s.length()-1).split(",");
        return new scala.Tuple2<String,Long>(parts[0], Long.parseLong(parts[1]));
    }
}
