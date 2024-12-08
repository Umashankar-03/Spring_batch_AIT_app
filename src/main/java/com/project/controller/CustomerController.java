package com.project.controller;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class CustomerController {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private Job  job;

    @Autowired
    private Job exportJob;

    @GetMapping("/import")
    public void loadData() throws Exception{
        JobParameters jobParams = new JobParametersBuilder()
                .addLong("startAt", System.currentTimeMillis())
                .toJobParameters();
        jobLauncher.run(job, jobParams);
    }

    // Endpoint to export data from the database to CSV
    @GetMapping("/export")
    public String exportData() {
        try {
            JobParameters jobParams = new JobParametersBuilder()
                    .addLong("startAt", System.currentTimeMillis())
                    .toJobParameters();
            jobLauncher.run(exportJob, jobParams);
            return "Export job started successfully. Check the file in 'src/main/resources/exported_customers.csv'.";
        } catch (Exception e) {
            e.printStackTrace();
            return "Export job failed.";
        }
    }

}
