
package cs455_final_project;

import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.split;
import java.util.*;

public class Main {
    public String getGreeting() {
        return "Hello World!";
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
            .appName("CS455 Project")
            .master("local[*]") // IMPORTANT!! LOCAL MODE! Comment out (or change?) when testing on HDFS cluster!
            .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        // NOTE: Below is a quick spark test (should work and print 0-10):
        // Dataset<Row> df = spark.range(0, 10).toDF("value");
        // df.show();

        String basePath = "src/main/resources";
        String coloradoPath = basePath + "/svi_county_GISJOIN.Colorado.raw_data.json";

        Dataset<Row> coloradoSVI = spark.read()
            .option("multiLine", true) // have to do this cause basically the file is a JSON array instead of a csv like HW4
            .json(coloradoPath);
            // .option("path", coloradoPath)
            // .load();
        coloradoSVI = coloradoSVI.cache();

        coloradoSVI.printSchema();
        coloradoSVI.show(5, false);

        spark.stop();
    }
}
