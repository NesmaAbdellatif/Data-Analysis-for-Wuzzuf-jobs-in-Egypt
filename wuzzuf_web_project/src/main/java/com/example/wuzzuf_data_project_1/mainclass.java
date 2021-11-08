package com.example.wuzzuf_data_project_1;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Controller
public class mainclass
{
    @RequestMapping("/homepage")
    public String homepage(Model model)
    {

        return "homepage";
    }

    String path = "Wuzzuf_Jobs.csv";
    Wuzzuf_DAO job = new Wuzzuf_DAO();
    final SparkSession sparkSession = SparkSession.builder ().appName ("Spark CSV Analysis Demo").master ("local[2]")
            .getOrCreate ();

    //1- Read data set and convert it to dataframe or Spark RDD and display some from it.
    Dataset<Row> JobsDF = job.ReadCSV(path);

    @RequestMapping("/task1")
    public String Task1(Model model)
    {
        System.out.println("1- Wuzzuf Dataframe \n");
        System.out.println("========================================================================");
        job.PrintDF(JobsDF, 10);

        List<String[]> displayData = new ArrayList<>();
        JobsDF.collectAsList().stream().forEach(row -> displayData.add(row.toString().split(",",8)));

        model.addAttribute("messages", displayData);

        return "JobsDF";
    }

    @RequestMapping("/task2")
    public String Task2(Model model)
    {

        List<String> summaryList = new ArrayList<>();
        JobsDF.describe().collectAsList().forEach(row -> summaryList.add(row.toString()));

        JobsDF.describe().show();

        model.addAttribute("message", summaryList);

        return "summary";
    }

    @RequestMapping("/task3")
    public String Task3(Model model)
    {
        List<String> arr_of_counts = new ArrayList<>();
        //3- Clean the data (null, duplications)
        System.out.println("3- Clean the data (null, duplications)  \n");
        System.out.println ("========================================================================");
        //before removing Duplicates and Nulls
        System.out.println("Number of rows before removing Duplicates and Nulls : "+JobsDF.count());
        String s1 = "" + JobsDF.count();
        arr_of_counts.add(s1);
        Dataset<Row> JobsDF_clean = job.clean_data(JobsDF);
        //After removing Duplicates
        System.out.println("Number of rows after removing Duplicates and Nulls : "+JobsDF_clean.count());
        System.out.println ("========================================================================");

        String s2 = "" + JobsDF_clean.count();
        arr_of_counts.add(s2);

        model.addAttribute("message", arr_of_counts);


        return "Cleaning";
    }
    @RequestMapping("/task4")
    public String Task4(Model model)
    {
        //4-Count the jobs for each company and display that in order (What are the most demanding companies for jobs?)
        System.out.println("4-Count the jobs for each company and display that in order (What are the most demanding companies for jobs?))  \n");
        System.out.println ("========================================================================");



        Dataset<Row> company_by_title =job.group_by_column(JobsDF ,"Company","Title" , sparkSession);
        job.PrintDF(company_by_title,10);

        List<String[]> displayData = new ArrayList<>();
        company_by_title.collectAsList().stream().forEach(row -> displayData.add(row.toString().split(",",8)));


        /*for (String[] strings : displayData) {
            System.out.println(Arrays.toString(strings));
        }*/
        model.addAttribute("messages", displayData);

        return "jobcount";
    }
    @RequestMapping("/task5")
    public String Task5(Model model)
    {

        Dataset<Row> company_by_title =job.group_by_column(JobsDF ,"Company","Title" , sparkSession);
        job.pie_chart(company_by_title,10,"Number of titles for each company ");
        return "task5";
    }
    @RequestMapping("/task6")
    public String Task6(Model model)
    {

        System.out.println("6- Find out What are it the most popular job titles? \n  ");
        System.out.println ("========================================================================");


        Dataset<Row> count_by_title =job.group_by_column(JobsDF ,"Title","Title" , sparkSession);
        job.PrintDF(count_by_title,10);

        List<String[]> displayData = new ArrayList<>();
        count_by_title.collectAsList().stream().forEach(row -> displayData.add(row.toString().split(",",8)));


        /*for (String[] strings : displayData) {
            System.out.println(Arrays.toString(strings));
        }*/
        model.addAttribute("messages", displayData);
        return "popularjob";
    }
    @RequestMapping("/task7")
    public String Task7(Model model)
    {

        Dataset<Row> count_by_title =job.group_by_column(JobsDF ,"Title","Title" , sparkSession);
        System.out.println("7-Show step 6 in bar chart \n  ");
        System.out.println ("========================================================================");

        job.bar_chart(count_by_title,  10 ,  "Title","Title","Job Title ", "Number of Titles","Job Tiitles"  );
        return "task7";
    }
    @RequestMapping("/task8")
    public String Task8(Model model)
    {

        //8- Find out the most popular areas?
        System.out.println("8- Find out the most popular areas?  \n ");
        System.out.println ("========================================================================");


        Dataset<Row> count_by_area =job.group_by_column(JobsDF ,"Location","Location" , sparkSession);
        job.PrintDF(count_by_area,10);
        List<String[]> displayData = new ArrayList<>();
        count_by_area.collectAsList().stream().forEach(row -> displayData.add(row.toString().split(",",8)));


        /*for (String[] strings : displayData) {
            System.out.println(Arrays.toString(strings));
        }*/
        model.addAttribute("messages", displayData);

            return "popularareas";
    }
    @RequestMapping("/task9")
    public String Task9(Model model)
    {

        Dataset<Row> count_by_area =job.group_by_column(JobsDF ,"Location","Location" , sparkSession);
        System.out.println("9- Show step 8 in bar chart \n  ");
        System.out.println ("========================================================================");
        job.bar_chart(count_by_area,  10 ,  "Location","Location","Locations ", "Most Popular","Locations"  );
        System.out.println ("========================================================================");
        return "task9";
    }
    @RequestMapping("/task11")
    public String Task11(Model model)
    {
        //11- Factorize the YearsExp feature and convert it to numbers in new col.
        System.out.println("11- Factorize the YearsExp feature and convert it to numbers in new col. \n ");
        System.out.println ("========================================================================");

        Dataset<Row> factorized_jobs = job.Factorize(JobsDF);

        factorized_jobs = factorized_jobs.drop("features");
        Dataset<Row> newDs = factorized_jobs.withColumn("skillsindex", factorized_jobs.col("Skills"));

        factorized_jobs=newDs.drop("Skills");


        List<String[]> displayData = new ArrayList<>();
        factorized_jobs.collectAsList().stream().forEach(row -> displayData.add(row.toString().split(",",11)));


        /*for (String[] strings : displayData) {
            System.out.println(Arrays.toString(strings));
        }*/
        model.addAttribute("message", displayData);

        return "factorize";
    }
    @RequestMapping("/task12")
    public String Task12(Model model)
    {
        Dataset<Row> factorized_jobs = job.Factorize(JobsDF);
        //12- Apply K-means for job title and companies
        System.out.println("12- Apply K-means for job title and companies \n ");
        System.out.println ("========================================================================");


        Dataset<Row> cluster = job.Kmeans(factorized_jobs);
        cluster = cluster.drop("features");
        Dataset<Row> newDs = cluster.withColumn("skillsindex", cluster.col("Skills"));

        cluster=newDs.drop("Skills");

        List<String[]> displayData = new ArrayList<>();
        cluster.collectAsList().stream().forEach(row -> displayData.add(row.toString().split(",",12)));

        model.addAttribute("message", displayData);

        return "kmeans";
    }
    @RequestMapping("/task10")
    public String Task10(Model model)
    {
        List<Map.Entry>  sorted=job.PopularSkills(path);
        List<String> Keys = new ArrayList<String>();
        List<Long> value = new ArrayList<Long>();


        // DISPLAY
        for (Map.Entry<String, Long> entry : sorted) {
            Keys.add(entry.getKey ());
            value.add(entry.getValue ());
        }

        model.addAttribute("message", Keys);
        model.addAttribute("messages", value);


        return "PopularSkills";
    }
}

