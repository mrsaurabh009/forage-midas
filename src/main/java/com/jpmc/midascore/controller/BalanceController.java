package com.jpmc.midascore.controller;

import com.jpmc.midascore.component.DatabaseConduit;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Balance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Optional;

@RestController
public class BalanceController {
    
    private static final Logger logger = LoggerFactory.getLogger(BalanceController.class);
    
    private final DatabaseConduit databaseConduit;
    
    public BalanceController(DatabaseConduit databaseConduit) {
        this.databaseConduit = databaseConduit;
    }
    
    @GetMapping("/balance")
    public Balance getBalance(@RequestParam Long userId) {
        logger.info("Querying balance for user ID: {}", userId);
        
        Optional<UserRecord> userRecord = databaseConduit.findUserById(userId);
        
        if (userRecord.isPresent()) {
            float balance = userRecord.get().getBalance();
            logger.info("Found user ID: {} with balance: {}", userId, balance);
            return new Balance(balance);
        } else {
            logger.info("User ID: {} not found, returning balance of 0", userId);
            return new Balance(0.0f);
        }
    }
}
