package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtils;
import org.jfree.chart.JFreeChart;
import org.jfree.data.category.DefaultCategoryDataset;

import java.io.File;

/**
 * Lab3_Potapkin
 *
 * @author Danila Potapkin
 * @since 18.12.2024
 */
public class Main {
	/**
	 * 10. SHIP Annual Season Influenza Vaccinations 2011-2021
	 */
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder()
			.appName("InfluenzaVaccinationAnalysis")
			.master("local[*]")
			.getOrCreate();

		String filePath = "./dataset.csv";
		Dataset<Row> data = spark.read().option("header", "true").csv(filePath);

		data = data.withColumn("Value", functions.col("Value").cast("double"))
			.withColumn("Year", functions.col("Year").cast("int"));

		data = data.filter("Value IS NOT NULL");

		System.out.println("Schema:");
		data.printSchema();

		System.out.println("Sample data:");
		data.show(10);

		// Task 1: Distribution by Year
		Dataset<Row> vaccinationByYear = data.groupBy("Year")
			.agg(functions.avg("Value").alias("average_vaccination"));
		vaccinationByYear.show();
		createBarChart(
			vaccinationByYear,
			"Year",
			"average_vaccination",
			"Average Vaccination by Year",
			"Year",
			"Average Vaccination",
			"vaccination_by_year.png"
		);

		// Task 2: Distribution by Race/Ethnicity
		Dataset<Row> vaccinationByRace = data.groupBy("Race/ ethnicity")
			.agg(functions.avg("Value").alias("average_vaccination"));
		vaccinationByRace.show();
		createBarChart(
			vaccinationByRace,
			"Race/ ethnicity",
			"average_vaccination",
			"Average Vaccination by Race/Ethnicity",
			"Race/Ethnicity",
			"Average Vaccination",
			"vaccination_by_race.png"
		);

		// Task 3: Distribution by Jurisdiction
		Dataset<Row> vaccinationByJurisdiction = data.groupBy("Jurisdiction")
			.agg(functions.avg("Value").alias("average_vaccination"));
		vaccinationByJurisdiction.show();
		createBarChart(
			vaccinationByJurisdiction,
			"Jurisdiction",
			"average_vaccination",
			"Average Vaccination by Jurisdiction",
			"Jurisdiction",
			"Average Vaccination",
			"vaccination_by_jurisdiction.png"
		);

		// Task 4: Year with highest vaccination rate
		Dataset<Row> maxVaccinationYear = data.groupBy("Year")
			.agg(functions.max("Value").alias("max_vaccination"))
			.orderBy(functions.desc("max_vaccination"));
		maxVaccinationYear.show(1);

		// Task 5: Impact on different groups
		Dataset<Row> groupedVaccination = data.groupBy("Year", "Race/ ethnicity")
			.agg(functions.avg("Value").alias("average_vaccination"));
		groupedVaccination.show();

		spark.stop();
	}

	private static void createBarChart(Dataset<Row> data, String categoryColumn, String valueColumn, String title, String categoryAxisLabel, String valueAxisLabel, String outputPath) {
		try {
			DefaultCategoryDataset dataset = new DefaultCategoryDataset();

			data.collectAsList().forEach(row -> {
				String category = row.getAs(categoryColumn).toString();
				Double value = row.getAs(valueColumn);
				dataset.addValue(value, valueAxisLabel, category);
			});

			JFreeChart chart = ChartFactory.createBarChart(
				title,
				categoryAxisLabel,
				valueAxisLabel,
				dataset
			);

			ChartUtils.saveChartAsPNG(new File(outputPath), chart, 800, 600);
			System.out.println("Chart saved to " + outputPath);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
