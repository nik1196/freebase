import org.apache.spark.api.java.function.PairFunction;

public class VertexPropertyNumberAssigner2 implements PairFunction<scala.Tuple2<String,String>, String, String> {
    public scala.Tuple2<String,String> call(scala.Tuple2<String,String> s){
        String [] parts = s._2().split("\\s+");
        Long valId = new Long(1);
        StringBuilder stringBuilder = new StringBuilder();
        for(String x:parts){
            String [] xparts = x.split("!");
            xparts[0] = "[" + xparts[0]+ ++valId+":String:";
            xparts[1] = xparts[1] + valId + ":String:";
            for(String y:xparts) {
                stringBuilder.append(y);
            }
            stringBuilder.append("]:");
        }
        stringBuilder.replace(stringBuilder.length()-1,stringBuilder.length(),"|");
        return new scala.Tuple2<String, String>(s._1(), "@" + String.valueOf(valId) + "%" + "[label:String:" + s._1() +"]:"+ stringBuilder.toString());
    }
}
