package com.project.config;

import com.project.entity.Customer;
import com.project.repository.CustomerRepository;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

@Configuration
@EnableBatchProcessing
public class ImportBatchConfig {

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    private final CustomerRepository customerRepository;

    public ImportBatchConfig(CustomerRepository customerRepository) {
        this.customerRepository = customerRepository;
    }

    // item reader bean :- (It is used to read the customer data from csv file)

    @Bean
    public FlatFileItemReader<Customer> customerReader(){

        FlatFileItemReader<Customer> itemReader = new FlatFileItemReader<Customer>();
        itemReader.setResource(new FileSystemResource("src/main/resources/customers.csv"));
        itemReader.setName("customer-item-read");
        itemReader.setLinesToSkip(1);
        itemReader.setLineMapper(lineMapper());

        return itemReader;

    }

    private LineMapper<Customer> lineMapper() {

        DefaultLineMapper<Customer> lineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames("id", "firstName", "lastName", "email", "gender", "contactNo","country","dob");

        BeanWrapperFieldSetMapper<Customer> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Customer.class);

        lineMapper.setFieldSetMapper(fieldSetMapper);
        lineMapper.setLineTokenizer(lineTokenizer);

        return lineMapper;

    }


    // item Processor bean :- (It is used to perform some operation on the data)

    @Bean
    public CustomerProcessor customerProcessors(){
        return new CustomerProcessor();
    }

    // item writer bean :- (It is used to write a data in the db table)

    @Bean
    public RepositoryItemWriter<Customer> customerWriter(){
        RepositoryItemWriter<Customer> itemWriter = new RepositoryItemWriter<>();
        itemWriter.setRepository(customerRepository);
        itemWriter.setMethodName("save");
        return itemWriter;
    }

    // step bean :- (Step represents Item reader, processor, writer)

    @Bean
    public Step step(){
        return  stepBuilderFactory.get("step-1").<Customer,Customer>chunk(10)
                .reader(customerReader())
                .processor(customerProcessors())
                .writer(customerWriter())
                .build();
    }



    // job bean :- (It is used to represent the step & job can be excepted by job launcher )

    @Bean
    public Job job (){
        return jobBuilderFactory.get("customer-import")
                .flow(step())
                .end()
                .build();

    }

}
