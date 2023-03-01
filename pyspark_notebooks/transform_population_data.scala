

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.length
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.SaveMode

object covid extends App {

 Logger.getLogger("org").setLevel(Level.ERROR)
            
val sparkConf= new SparkConf()
    sparkConf.set("spark.app.name","covid_problems")
    sparkConf.set("spark.master","local[*]")
    
val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()
    
    
val df1= spark.read.format("csv")
         .option("sep", "\t")
         .option("inferschema",true)
         .option("header",true)
         .option("path","/Users/vishe/Downloads/population_by_age.tsv")
         .load()
 
val df2=df1.withColumn("indic_de,geo\\time",regexp_replace(col("indic_de,geo\\time"),"PC_",""))
 
 
val df3=df2.select(split(col("indic_de,geo\\time"),",").getItem(0).as("agegroup"),split(col("indic_de,geo\\time"),",").getItem(1).as("country_code"),col("2019 ").as("Percentage_2019"))

val df4=df3.withColumn("Percentage_2019",regexp_replace(col("Percentage_2019"),"[a-z]","").cast("Double")).filter(length(col("country_code")) ===2).groupBy("country_code").pivot("agegroup").sum("Percentage_2019").orderBy("country_code").cache()

val df5= spark.read.format("csv")
         .option("sep", ",")
         .option("inferschema",true)
         .option("header",true)
         .option("path","/Users/vishe/Downloads/country_lookup.csv")
         .load()
         
val df6=df4.join(df5,df4.col("country_code")===df5.col("country_code_2_digit"),"left").na.drop(Seq("country")).drop("country_code").orderBy("country").select("country","country_code_2_digit","country_code_3_digit","population","Y0_14","Y15_24","Y25_49","Y50_64","Y65_79","Y80_MAX")

df6.coalesce(1).write.format("csv").option("header",true).mode(SaveMode.Overwrite).save("/Users/vishe/Downloads/finaldata")
}
