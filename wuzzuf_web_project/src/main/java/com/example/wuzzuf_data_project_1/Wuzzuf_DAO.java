package com.example.wuzzuf_data_project_1;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.*;
import org.knowm.xchart.*;
import org.knowm.xchart.style.Styler;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author nesmaabdellatif
 */
public class Wuzzuf_DAO {
        private static final String COMMA_DELIMITER = ",";
        public List<Wuzzuf_POJO> read_jobs(String path){
		
	    List<Wuzzuf_POJO>  job_list= new ArrayList<Wuzzuf_POJO>() ;
            File Wuzzuf = new File(path);
            List <String> records;
            records = new ArrayList<String>() ;
            try {
                records = (List<String>) Files.readAllLines(Wuzzuf.toPath());


            }
            catch(Exception e){
            System.out.println("An Issue Has been Occured " + e);}

            for(int index=1 ; index< records.size() ; index++){
                String record = records.get(index);
                String[] fields = record.split(",");

                Wuzzuf_POJO new_jop = create_job(fields);
                job_list.add(new_jop);
            }
            return job_list;
        }
        public Wuzzuf_POJO create_job(String[] fields ){
            String title=fields[0].trim(); 
            String company=fields[1].trim(); 
            String location=fields[2].trim(); 
            String type=fields[3].trim(); 
            String level=fields[4].trim(); 
            String years_exp=fields[5].trim(); 
            String country=fields[6].trim(); 
            String[] skills=fields[7].split(",");

   
            Wuzzuf_POJO new_jop = new Wuzzuf_POJO(title ,  company,  location,level,  type,  years_exp,  country,skills );
            return new_jop;
        }
        public Dataset<Row> ReadCSV(String path){
            // Create Spark Session to create connection to Spark
            final SparkSession sparkSession = SparkSession.builder ().appName ("Spark CSV Analysis Demo").master ("local[2]")
                    .getOrCreate ();

            // Get DataFrameReader using SparkSession
            final DataFrameReader dataFrameReader = sparkSession.read ().format("libsvm");
            // Set header option to true to specify that first row in file contains
            // name of columns
            dataFrameReader.option ("header", "true");
            final Dataset<Row> JobsDF = dataFrameReader.csv (path);

            // Print Schema to see column names, types and other metadata
            //JobsDF.printSchema ();
            return JobsDF;
        }
        
        public void PrintDF(Dataset<Row> JobsDF, Integer n){
            JobsDF.show(n);
        }
        public void PrintSummary(Dataset<Row> JobsDF){
            System.out.println("Schema: \n");
            JobsDF.printSchema();
            System.out.println("Description : \n");
            JobsDF.describe().show();
            
        }
        public Dataset<Row> clean_data(Dataset<Row> JobsDF){
		Dataset<Row> New_Data=JobsDF.dropDuplicates();
		New_Data=New_Data.na().drop();
		return New_Data;
	}
        public Dataset<Row> group_by_column(Dataset<Row> JobsDF , String column1,String column2 ,SparkSession sparkSession){
	    // Create view and execute query to convert types as, by default, all
	    // columns have string types
	    JobsDF.createOrReplaceTempView("table");
            String query="SELECT "+column1+", COUNT("+column2+")"
	    		+ " FROM table "
	    		+ "GROUP BY "+column1
	    		+ " ORDER BY COUNT("+column2+")"+ " DESC";
	    Dataset<Row> group_by = sparkSession.sql(query);
	    return group_by;
	    
	}
        public void pie_chart(Dataset<Row> Data_grouped , int num , String title)
        {
            List<Row> count_list =Data_grouped.collectAsList();
	    PieChart chart = new PieChartBuilder ().width (1000).height (2000).title (title).build ();
	    count_list.stream().limit(num).forEach(t->{chart.addSeries(t.getAs(0), t.getAs(1));});
	     
	     // Series
	     // Show it
	     //new SwingWrapper (chart).displayChart ();
            try {
                BitmapEncoder.saveBitmapWithDPI(chart, title, BitmapEncoder.BitmapFormat.PNG,80);
            } catch (IOException e) {
                System.out.println("TAAAAAAAAAAAHHHHHHHHHHHHHHHHAAAAAAAAAAAAAAAAAAAAA");
                e.printStackTrace();
            }
        }

        public void bar_chart(Dataset<Row> Data_grouped , int num ,  String column1,String column2,String titlex , String titley,String title  ){
		
            List<String> col1_list = Data_grouped.select(column1).as(Encoders.STRING()).limit(num).collectAsList();
	    List<Float> col2_list = Data_grouped.select( "count("+column2+")").as(Encoders.FLOAT()).limit(num).collectAsList();
            // Create Chart
            CategoryChart bar = new CategoryChartBuilder ().width (1000).height (2000).title (title).xAxisTitle (titlex).yAxisTitle (titley).build ();
            // Customize Chart
            bar.getStyler ().setLegendPosition (Styler.LegendPosition.InsideNW);
            bar.getStyler ().setHasAnnotations (true);
            bar.getStyler ().setStacked (true);
            // Series
            bar.addSeries (title, col1_list, col2_list);
            // Show it
            //new SwingWrapper (bar).displayChart ();
            try {
                BitmapEncoder.saveBitmapWithDPI(bar, title, BitmapEncoder.BitmapFormat.PNG,80);
            } catch (IOException e) {
                e.printStackTrace();
            }
		
	}
        public List<Map.Entry> PopularSkills (String path){
            // CREATE SPARK CONTEXT
            SparkConf conf = new SparkConf ().setAppName ("jobs").setMaster ("local[3]"); 
            conf.set("spark.driver.allowMultipleContexts", "true");
            JavaSparkContext sparkContext = new JavaSparkContext (conf);
            
            // LOAD DATASETS
            JavaRDD<String> jobs = sparkContext.textFile (path);  
            
            // TRANSFORMATIONS
            JavaRDD<String> skills = jobs
                    .map (Wuzzuf_DAO::extractSkills)
                    .filter (StringUtils::isNotBlank);
            
            // JavaRDD<String>
            JavaRDD<String> words = skills.flatMap (title -> Arrays.asList (title
                    .toLowerCase ()
                    .trim ()
                    .replaceAll ("\\p{Punct}", " ")
                    .split (" ")).iterator ());
            
            // COUNTING
            Map<String, Long> SkillsCounts = words.countByValue ();
       
            List<Map.Entry> sorted = SkillsCounts.entrySet ().stream ()
                   .sorted (Map.Entry.comparingByValue ()).collect (Collectors.toList ());
            
            return sorted;
        }
        public Dataset<Row> Factorize (Dataset<Row> JobsDF){
            StringIndexer indexer = new StringIndexer();
            StringIndexer indexer1 = new StringIndexer();
            StringIndexer indexer2 = new StringIndexer();
            
            indexer.setInputCol("YearsExp").setOutputCol("YearsExpIndex");
            indexer1.setInputCol("Title").setOutputCol("TitleIndexed");
            indexer2.setInputCol("Company").setOutputCol("CompanyIndexed");

            Dataset<Row> indexed  = indexer.fit(JobsDF).transform(JobsDF);
            Dataset<Row> indexed1 = indexer1.fit(indexed).transform(indexed);
            Dataset<Row> indexed2 = indexer2.fit(indexed1).transform(indexed1);

            return indexed2;
        }
        public Dataset<Row> Kmeans (Dataset<Row> JobsDF){
            
            //Transform DF
            String inputColumns[] = {"TitleIndexed", "CompanyIndexed"};
            VectorAssembler vectorAssembler = new VectorAssembler();
            vectorAssembler.setInputCols(inputColumns);
            vectorAssembler.setOutputCol("features");
            
            Dataset<Row> JobsTrain = vectorAssembler.transform(JobsDF);
            JobsTrain.show();
            
            // Trains a k-means model.
            KMeans kmeans = new KMeans().setK(2).setSeed(1L);
            kmeans.setFeaturesCol("features");
            kmeans.setPredictionCol("Clusters");
            KMeansModel model = kmeans.fit(JobsTrain);
            
            // Evaluate clustering by computing Within Set Sum of Squared Errors.
            double WSSSE = model.computeCost(JobsTrain);
            System.out.println("Within Set Sum of Squared Errors = " + WSSSE);
            
            // Cluster centers
            for (org.apache.spark.ml.linalg.Vector center : model.clusterCenters()) {
            System.out.println(" " + center);
           }
            
            //k-means clusters
            Dataset<Row> cluster = model.transform(JobsTrain);

            cluster.show();
            return cluster;
        }
        public static String extractSkills(String skills) {
            try {
                return skills.split (COMMA_DELIMITER)[7];
            } catch (ArrayIndexOutOfBoundsException e) {
                return "";
            }
        }   
        
        
}
        
        
    

