import org.apache.spark.api.java.function.PairFunction;

public class EdgePropertyNumberAssignerMapper implements PairFunction<scala.Tuple2<scala.Tuple2<String,String>,String>, scala.Tuple2<String,String>, String> {
    public scala.Tuple2<scala.Tuple2<String,String>,String> call(scala.Tuple2<scala.Tuple2<String,String>,String> s){
        Long propertyNumber = new Long(0);
        StringBuilder stringBuilder= new StringBuilder(",");
        for(int i=0;i<s._2().length(); i++){
            if(s._2().charAt(i)=='<') {
                if(i>0)
                    stringBuilder.append("$");
                stringBuilder.append("p" + String.valueOf(++propertyNumber)+":String:");
            }
            stringBuilder.append(s._2().charAt(i));
        }
        stringBuilder.append("$");
        return new scala.Tuple2<scala.Tuple2<String,String>,String>(s._1(), stringBuilder.toString());
    }
}
