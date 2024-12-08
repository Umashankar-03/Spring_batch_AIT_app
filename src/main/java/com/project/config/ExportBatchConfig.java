package com.project.config;

import com.project.entity.Customer;
import com.project.repository.CustomerRepository;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.data.RepositoryItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.data.domain.Sort;

import java.util.Map;

@Configuration
@EnableBatchProcessing
public class ExportBatchConfig {

    @Autowired
    private CustomerRepository customerRepository;

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    // item reader

    @Bean
    public RepositoryItemReader<Customer> customerDBReader (){
        RepositoryItemReader<Customer> itemReader = new RepositoryItemReader<>();
        itemReader.setRepository(customerRepository);
        itemReader.setMethodName("findAll");
        itemReader.setPageSize(1000);
        itemReader.setSort(Map.of("id", Sort.Direction.ASC));

        return itemReader;
    }

    // item processor

    @Bean
    public CustomerProcessor customerProcessor(){
        return new CustomerProcessor();
    }

    // item Writer

    @Bean
    public FlatFileItemWriter<Customer> customCsvWriter(){
        FlatFileItemWriter<Customer> itemWriter = new FlatFileItemWriter<>();
        itemWriter.setResource(new FileSystemResource("src/main/resources/exported_customers.csv"));
        itemWriter.setName("custom-csv-writer");
        itemWriter.setHeaderCallback( header ->
                header.write("id,firstName,lastName,email,gender,contactNo,country,dob"));
        itemWriter.setLineAggregator(new DelimitedLineAggregator<Customer>(){{
            setDelimiter(",");
            setFieldExtractor( new BeanWrapperFieldExtractor<Customer>(){{
                setNames(new String[]{"id", "firstName", "lastName", "email", "gender", "contactNo", "country", "dob"});
            }});
        }});

        return itemWriter;

    }

    // item step

    @Bean
    public Step exportStep(){
        return stepBuilderFactory.get("export-step").<Customer, Customer>chunk(10)
                .reader(customerDBReader())
                .processor(customerProcessor())
                .writer(customCsvWriter())
                .build();
    }

    // Job

    @Bean
    public Job exportJob(){
        return jobBuilderFactory.get("export-job")
                .flow(exportStep())
                .end()
                .build();
    }



}
