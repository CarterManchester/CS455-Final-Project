package svi_prison_analysis.ML;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.*;
// import org.apache.spark.ml.*;
// import org.apache.spark.ml.evaluation.RegressionEvaluator;
// import org.apache.spark.ml.feature.VectorAssembler;
// import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class PrisonClassificationModel {
    
    public static void trainAndEvaluate(Dataset<Row> data){
        // Dataset<Row>filtered = data.filter(col("incarceration_rate").isNotNull());

        // String[] featureCols = new String[] {
        //     "RPL_THEMES",
        //     "RPL_THEME1",
        //     "RPL_THEME2",
        //     "RPL_THEME3",
        //     "EP_POV",
        //     "EP_UNEMP",
        //     "EP_NOHSDP",
        //     "EP_MINRTY",
        //     "EP_LIMENG",
        //     "EP_GROUPQ",
        // };


        // VectorAssembler assembler = new VectorAssembler()
        //     .setInputCols(featureCols)
        //     .setOutputCol("features_raw");
        
        // LinearRegression lr = new LinearRegression()
        //     .setLabelCol("incarceration_rate")
        //     .setFeaturesCol("features")
        //     .setMaxIter(100)
        //     .setRegParam(0.1);

        // //train/test split
        // Dataset<Row>[] splits = filtered.randomSplit(new double[]{0.8, 0.2}, 42);
        // Dataset<Row> train = splits[0];
        // Dataset<Row> test = splits[1];
    }
}
