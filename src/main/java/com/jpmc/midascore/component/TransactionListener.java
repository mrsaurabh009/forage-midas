package com.jpmc.midascore.component;

import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TransactionListener {
    
    private static final Logger logger = LoggerFactory.getLogger(TransactionListener.class);
    private static int transactionCount = 0;
    
    @KafkaListener(topics = "${general.kafka-topic}")
    public void listen(Transaction transaction) {
        transactionCount++;
        logger.info("=== TRANSACTION #{} RECEIVED === Amount: {} from {} to {}", 
                   transactionCount, transaction.getAmount(), transaction.getSenderId(), transaction.getRecipientId());
        logger.info("Full transaction details: {}", transaction);
        
        // Print first 4 transactions amounts for easy identification
        if (transactionCount <= 4) {
            logger.warn(">>> FIRST 4 TRANSACTIONS - #{}: AMOUNT = {} <<<", transactionCount, transaction.getAmount());
        }
    }
}
