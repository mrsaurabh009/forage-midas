package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

@Component
public class TransactionListener {
    
    private static final Logger logger = LoggerFactory.getLogger(TransactionListener.class);
    private static int transactionCount = 0;
    private final DatabaseConduit databaseConduit;
    private final IncentiveService incentiveService;
    
    public TransactionListener(DatabaseConduit databaseConduit, IncentiveService incentiveService) {
        this.databaseConduit = databaseConduit;
        this.incentiveService = incentiveService;
    }
    
    @KafkaListener(topics = "${general.kafka-topic}")
    @Transactional
    public void listen(Transaction transaction) {
        transactionCount++;
        logger.info("=== TRANSACTION #{} RECEIVED === Amount: {} from {} to {}", 
                   transactionCount, transaction.getAmount(), transaction.getSenderId(), transaction.getRecipientId());
        
        // Validate and process the transaction
        if (processTransaction(transaction)) {
            logger.info("Transaction #{} PROCESSED successfully", transactionCount);
        } else {
            logger.warn("Transaction #{} REJECTED - validation failed", transactionCount);
        }
    }
    
    private boolean processTransaction(Transaction transaction) {
        try {
            // Find sender and recipient
            Optional<UserRecord> sender = databaseConduit.findUserById(transaction.getSenderId());
            Optional<UserRecord> recipient = databaseConduit.findUserById(transaction.getRecipientId());
            
            // Validate sender exists
            if (sender.isEmpty()) {
                logger.warn("Invalid sender ID: {}", transaction.getSenderId());
                return false;
            }
            
            // Validate recipient exists
            if (recipient.isEmpty()) {
                logger.warn("Invalid recipient ID: {}", transaction.getRecipientId());
                return false;
            }
            
            // Validate sender has sufficient balance
            if (sender.get().getBalance() < transaction.getAmount()) {
                logger.warn("Insufficient balance. Sender {} has {} but needs {}", 
                           sender.get().getName(), sender.get().getBalance(), transaction.getAmount());
                return false;
            }
            
            // All validations passed - process the transaction
            // Get incentive from API
            float incentiveAmount = incentiveService.getIncentiveAmount(transaction);
            
            // Calculate new balances
            float newSenderBalance = sender.get().getBalance() - transaction.getAmount();
            float newRecipientBalance = recipient.get().getBalance() + transaction.getAmount() + incentiveAmount;
            
            // Update balances
            databaseConduit.updateUserBalance(sender.get().getId(), newSenderBalance);
            databaseConduit.updateUserBalance(recipient.get().getId(), newRecipientBalance);
            
            // Create and save transaction record with incentive
            TransactionRecord transactionRecord = new TransactionRecord(sender.get(), recipient.get(), transaction.getAmount(), incentiveAmount);
            databaseConduit.saveTransaction(transactionRecord);
            
            logger.info("Transaction processed: {} -> {} amount: {}, incentive: {} (New balances: {} = {}, {} = {})",
                       sender.get().getName(), recipient.get().getName(), transaction.getAmount(), incentiveAmount,
                       sender.get().getName(), newSenderBalance,
                       recipient.get().getName(), newRecipientBalance);
            
            return true;
        } catch (Exception e) {
            logger.error("Error processing transaction: ", e);
            return false;
        }
    }
}
