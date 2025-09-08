package com.jpmc.midascore.component;

import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class IncentiveService {
    
    private static final Logger logger = LoggerFactory.getLogger(IncentiveService.class);
    private static final String INCENTIVE_API_URL = "http://localhost:8080/incentive";
    
    private final RestTemplate restTemplate;
    
    public IncentiveService(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }
    
    public float getIncentiveAmount(Transaction transaction) {
        try {
            logger.info("Calling incentive API for transaction: {}", transaction);
            Incentive incentive = restTemplate.postForObject(INCENTIVE_API_URL, transaction, Incentive.class);
            
            if (incentive != null) {
                float incentiveAmount = incentive.getAmount();
                logger.info("Received incentive amount: {} for transaction {}", incentiveAmount, transaction);
                return incentiveAmount;
            } else {
                logger.warn("Received null incentive response for transaction: {}", transaction);
                return 0.0f;
            }
        } catch (Exception e) {
            logger.error("Error calling incentive API for transaction: " + transaction, e);
            return 0.0f; // Return 0 if API call fails
        }
    }
}
