import org.apache.spark.api.java.function.PairFunction;

public class VertexPropertyNumberAssigner implements PairFunction<scala.Tuple2<String,String>, String, String> {
    public scala.Tuple2<String,String> call(scala.Tuple2<String,String> s){
        String [] parts = s._2().split("\\s+");
        Long valId = new Long(0);
        StringBuilder stringBuilder = new StringBuilder();
        for(String x:parts){
            String [] xparts = x.split("!");
            xparts[0] = xparts[0]+ ++valId+":String:";
            xparts[1] = xparts[1] + valId + ":String:";
            for(String y:xparts)
                stringBuilder.append(y);
        }
        return new scala.Tuple2<String, String>(s._1(), stringBuilder.toString());
    }
}
