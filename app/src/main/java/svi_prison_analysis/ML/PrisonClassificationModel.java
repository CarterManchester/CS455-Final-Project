package svi_prison_analysis.ML;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.*;
import org.apache.spark.ml.*;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.FeatureHasher;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.regression.GBTRegressionModel;
import org.apache.spark.ml.regression.GBTRegressor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import java.util.*;

public class PrisonClassificationModel {
    
    public static void trainAndEvaluate(Dataset<Row> data){
        Dataset<Row> filtered = data.filter(col("incarceration_rate").isNotNull());
        
        String[] allColumnNames = filtered.columns();

        List<String> tempList = Arrays.asList(allColumnNames);
        List<String> listColumnNames = new ArrayList<>(tempList);
        listColumnNames.remove("incarceration_rate");
        String[] featureCols = listColumnNames.toArray(new String[0]);

        System.out.println("Creating VectorAssembler and LinearRegression Objects for Model");
        
            VectorAssembler assembler = new VectorAssembler()
            .setInputCols(featureCols)
            .setOutputCol("features");
            
        Dataset<Row> transformedTrainData = assembler.transform(data);

        Dataset<Row>[] splits = transformedTrainData.randomSplit(new double[]{0.7, 0.3});
        Dataset<Row> trainingData = splits[0];
        Dataset<Row> testData = splits[1]; 
        
        GBTRegressor gbt = new GBTRegressor()
            .setFeaturesCol("features")
            .setLabelCol("incarceration_rate");

        GBTRegressionModel model = gbt.fit(transformedTrainData);

        // Make predictions.
        Dataset<Row> predictions = model.transform(testData);
        
        Vector vector = model.featureImportances();

        double[] weights = vector.toArray();

        // Ensure the number of weights matches the number of input features
        if (weights.length != featureCols.length) {
            System.err.println("Error: Importance vector size (" + weights.length + 
                               ") does not match feature column count (" + featureCols.length + 
                               "). Check feature processing.");
            return;
        }

        for (int i = 0; i < featureCols.length; i++) {
            System.out.printf("%-20s : %.6f%n", featureCols[i], weights[i]);
        }
    }
}
