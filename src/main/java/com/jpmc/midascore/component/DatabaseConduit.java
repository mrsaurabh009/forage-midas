package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

@Component
public class DatabaseConduit {
    private final UserRepository userRepository;
    private final TransactionRepository transactionRepository;

    public DatabaseConduit(UserRepository userRepository, TransactionRepository transactionRepository) {
        this.userRepository = userRepository;
        this.transactionRepository = transactionRepository;
    }

    public void save(UserRecord userRecord) {
        userRepository.save(userRecord);
    }

    public Optional<UserRecord> findUserById(Long id) {
        return userRepository.findById(id);
    }

    public Optional<UserRecord> findUserByName(String name) {
        return userRepository.findByName(name);
    }

    @Transactional
    public void saveTransaction(TransactionRecord transactionRecord) {
        transactionRepository.save(transactionRecord);
    }

    @Transactional
    public void updateUserBalance(Long userId, float newBalance) {
        Optional<UserRecord> user = userRepository.findById(userId);
        if (user.isPresent()) {
            user.get().setBalance(newBalance);
            userRepository.save(user.get());
        }
    }
}
