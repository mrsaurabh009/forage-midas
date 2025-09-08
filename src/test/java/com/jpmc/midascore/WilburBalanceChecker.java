package com.jpmc.midascore;

import com.jpmc.midascore.component.DatabaseConduit;
import com.jpmc.midascore.entity.UserRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
public class WilburBalanceChecker {
    
    private static final Logger logger = LoggerFactory.getLogger(WilburBalanceChecker.class);
    
    @Autowired
    private DatabaseConduit databaseConduit;
    
    public void checkWilburBalance() {
        // Wilbur is user ID 9 based on the test data
        Optional<UserRecord> wilburOpt = databaseConduit.findUserById(9L);
        
        if (wilburOpt.isPresent()) {
            UserRecord wilbur = wilburOpt.get();
            logger.info("=== WILBUR'S FINAL BALANCE ===");
            logger.info("User ID: {}", wilbur.getId());
            logger.info("Name: {}", wilbur.getName());
            logger.info("Balance: {}", wilbur.getBalance());
            logger.info("Rounded down balance: {}", (int) Math.floor(wilbur.getBalance()));
            logger.info("==============================");
            
            // Also log to console
            System.out.println("=== WILBUR'S FINAL BALANCE ===");
            System.out.println("User ID: " + wilbur.getId());
            System.out.println("Name: " + wilbur.getName());
            System.out.println("Balance: " + wilbur.getBalance());
            System.out.println("Rounded down balance: " + (int) Math.floor(wilbur.getBalance()));
            System.out.println("==============================");
        } else {
            logger.error("Could not find wilbur (user ID 9)!");
            System.out.println("Could not find wilbur (user ID 9)!");
        }
    }
}
