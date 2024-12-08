package com.project.config;

import com.project.entity.Customer;
import org.springframework.batch.item.ItemProcessor;


public class CustomerProcessor implements ItemProcessor<Customer , Customer> {

    @Override
    public Customer process(Customer item) throws Exception {
        // logic to process data.
    /*  If i want to store only indian people data
        if(item.getCountry().equals("India")){
            return item;
        }else {
            return null;
        }

     */

        return item;
    }
}
