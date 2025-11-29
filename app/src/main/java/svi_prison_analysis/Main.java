
package svi_prison_analysis;

import org.apache.spark.sql.*;
// import static org.apache.spark.sql.functions.*;
// import static org.apache.spark.sql.functions.split;
// import java.util.*;

import svi_prison_analysis.States.*;

public class Main {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
            .appName("Investigating Correlations Between Social Vulnerability and Prison Populations")
            .master("local[*]") // IMPORTANT!! LOCAL MODE! Comment out (or change?) when testing on HDFS cluster!
            .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");
        spark.conf().set("spark.sql.debug.maxToStringFields", "200"); //NOTE: This is so it prints strings without size warnings. Change back if needed!!

        // NOTE: quick spark test to see if spark is running (print 0-10):
        // Dataset<Row> df = spark.range(0, 10).toDF("value");
        // df.show();

        
        //maybe we can find a faster/cleaner way to go through each state?
        State colorado = new Colorado(spark);
        Illinois illinois = new Illinois(spark);
        colorado.runSVI();
        illinois.runSVI();

        spark.stop();
    }
}
