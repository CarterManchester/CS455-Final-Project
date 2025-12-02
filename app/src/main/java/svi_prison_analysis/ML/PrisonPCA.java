package svi_prison_analysis.ML;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.*;
import org.apache.spark.ml.*;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.PCA;
import org.apache.spark.ml.feature.PCAModel;
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


import java.util.*;


public class PrisonPCA {
    
    /**
     * Run PCA on data 
     * 
     * @param data combined dataset with SVI features + incarceration_rate
     * @param featureCols names of feature columns to use in PCA
     * @param k number of principal components
     */
    public static void runPCA(Dataset<Row> data, String[] featureCols, int k){
        data = data.filter(col("incarceration_rate").isNotNull());

        VectorAssembler assembler = new VectorAssembler()
            .setInputCols(featureCols)
            .setOutputCol("features_raw");

        StandardScaler scaler = new StandardScaler() //looks like PCA works better on standardized data?
            .setInputCol("features_raw")
            .setOutputCol("features")
            .setWithMean(true)
            .setWithStd(true); //standard deviation

        PCA pca = new PCA()
            .setInputCol("features")
            .setOutputCol("pcaFeatures")
            .setK(k);

        Pipeline pipeline = new Pipeline() 
            .setStages(new PipelineStage[]{assembler, scaler, pca});

        PipelineModel pipelineModel = pipeline.fit(data);

        Dataset<Row> transformed = pipelineModel.transform(data);

        PCAModel pcaModel = (PCAModel) pipelineModel.stages()[2];




        System.out.println("PCA Explained Variance (per component)");
        System.out.println(pcaModel.explainedVariance());

        System.out.println("Sample of Counties in PCA space: ");
        transformed.select(
            col("STATE"),
            col("COUNTY"),
            col("incarceration_rate"),
            col("pcaFeatures")
        ).show(20, false);

    }

}
