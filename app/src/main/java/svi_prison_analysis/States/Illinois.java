package svi_prison_analysis.States;
import svi_prison_analysis.State;

import org.apache.spark.sql.*;


public class Illinois extends State{
    public Illinois (SparkSession spark){
        super(spark);
    }

    @Override
    protected String getStateName() {
        return "Illinois";
    }

}
