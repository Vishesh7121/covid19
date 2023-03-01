


  

import org.apache.log4j.Level
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.length
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types._
object mypro2 extends App {

 Logger.getLogger("org").setLevel(Level.ERROR)
            
val sparkConf= new SparkConf()
    sparkConf.set("spark.app.name","covid_problems")
    sparkConf.set("spark.master","local[*]")
    
val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()
    
    
val df1= spark.read.format("csv")
         .option("sep", ",")
         .option("inferschema",true)
         .option("header",true)
         .option("path","/Users/vishe/Downloads/dim_date.csv")
         .load()
val df4=df1.withColumnRenamed("date","the_date")

val df2= spark.read.format("csv")
         .option("sep", ",")
         .option("inferschema",true)
         .option("header",true)
         .option("path","/Users/vishe/Downloads/country_lookup.csv")
         .load()
 val df3= spark.read.format("csv")
         .option("sep", ",")
         .option("inferschema",true)
         .option("header",true)
         .option("path","/Users/vishe/Downloads/testing.csv")
         .load()
         
df4.show()
df2.show()
df3.show()

 
 df4.createOrReplaceTempView("dim_time")
  df2.createOrReplaceTempView("country_lookup")
   df3.createOrReplaceTempView("testing_data")
 
 spark.sql("""
      select t.country  ,
      c.country_code_2_digit   ,
      c.country_code_3_digit   ,
      t.year_week   ,
      
      MIN(d.the_date) AS week_start_date, 
      MAX(d.the_date) AS week_end_date,
      t.new_cases   ,
      t.tests_done  ,
      t.population  , 
      t.testing_rate   ,
      t.positivity_rate   ,
      t.testing_data_source 
      
     from testing_data t join dim_time d on t.year_week=concat(cast(d.year as varchar(5)),'-W',lpad(cast(d.week_of_year as varchar(3)),2,'00'))
     join country_lookup c 
   ON (t.country_code = c.country_code_2_digit)   
   
   where t.year_week=="2020-W43"
   group by t.country  ,
      c.country_code_2_digit   ,
      c.country_code_3_digit   ,
      t.year_week   ,
      t.new_cases   ,
      t.tests_done  ,
      t.population  , 
      t.testing_rate   ,
      t.positivity_rate   ,
      t.testing_data_source 
      order by t.country,t.year_week
      
      
   
     """).show(100)
     
 //df5.coalesce(1).write.format("csv").option("header",true).mode(SaveMode.Overwrite).save("/Users/vishe/Downloads/finaltestingresults")

}