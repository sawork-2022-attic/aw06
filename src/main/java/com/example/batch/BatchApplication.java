package com.example.batch;

import com.example.batch.model.Product;
import com.example.batch.service.JsonReader;
import com.example.batch.service.ProductProcessor;
import com.example.batch.service.ProductWriter;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
@EnableBatchProcessing
public class BatchApplication {

	@Autowired
	private JobBuilderFactory jobs;

	@Autowired private StepBuilderFactory steps;



	@Bean
	public JobLauncher jobLauncher() throws Exception {
		SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
		return jobLauncher;
	}

	@Bean
	public ItemReader<JsonNode> itemReader() {
		return new JsonReader();
	}

	@Bean
	public ItemProcessor<JsonNode, Product> itemProcessor() {
		return new ProductProcessor();
	}

	@Bean
	public ItemWriter<Product> itemWriter() {
		return new ProductWriter();
	}

	@Bean
	protected Step processLines(ItemReader<JsonNode> reader, ItemProcessor<JsonNode, Product> processor, ItemWriter<Product> writer) {
		return steps.get("processProducts").<JsonNode, Product> chunk(2)
				.reader(reader)
				.processor(processor)
				.writer(writer)
				.build();
	}

	@Bean
	public Job job() {
		return jobs
				.get("chunksJob")
				.start(processLines(itemReader(), itemProcessor(), itemWriter()))
				.build();
	}

	public static void main(String[] args) {
		SpringApplication.run(BatchApplication.class, args);
	}

}
