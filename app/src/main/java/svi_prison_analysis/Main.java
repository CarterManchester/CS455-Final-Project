
package svi_prison_analysis;

import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.split;
import java.util.*;

public class Main {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
            .appName("Investigating Correlations Between Social Vulnerability and Prison Populations")
            .master("local[*]") // IMPORTANT!! LOCAL MODE! Comment out (or change?) when testing on HDFS cluster!
            .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        // NOTE: Below is a quick spark test (should work and print 0-10):
        // Dataset<Row> df = spark.range(0, 10).toDF("value");
        // df.show();

        
        Colorado colorado = new Colorado(spark);
        colorado.runSVI();

        spark.stop();
    }
}
