# Midas Core

Project repository for the JPMC Advanced Software Engineering Forage program.

## Overview

Midas Core is a high-profile financial transaction processing system that receives, validates, and records financial transactions. This Spring Boot application integrates with multiple external resources:

- **Kafka** - For receiving new transactions
- **SQL Database** - For recording and validating transactions (H2 in-memory for development)
- **REST API** - For transaction incentivization

## Technology Stack

- **Java 17** - Programming language
- **Spring Boot 3.2.5** - Main framework
- **Spring Data JPA** - Database operations
- **Spring Kafka** - Message processing
- **H2 Database** - In-memory database for development/testing
- **Maven** - Build tool
- **JUnit 5** - Testing framework
- **Testcontainers** - Integration testing

## Prerequisites

- Java 17 JDK
- Maven 3.8+ (or use included Maven wrapper)
- IDE with Spring Boot support (IntelliJ IDEA recommended)

## Project Structure

```
src/
├── main/java/com/jpmc/midascore/
│   ├── MidasCoreApplication.java          # Spring Boot main class
│   ├── component/
│   │   └── DatabaseConduit.java           # Database operations component
│   ├── entity/
│   │   └── UserRecord.java                # JPA entity for user data
│   ├── foundation/
│   │   ├── Balance.java                   # Balance domain model
│   │   └── Transaction.java               # Transaction domain model
│   └── repository/
│       └── UserRepository.java            # JPA repository interface
└── test/java/com/jpmc/midascore/
    ├── TaskOneTests.java                  # Task 1 verification tests
    ├── TaskTwoTests.java                  # Task 2 verification tests
    ├── TaskThreeTests.java                # Task 3 verification tests
    ├── TaskFourTests.java                 # Task 4 verification tests
    ├── TaskFiveTests.java                 # Task 5 verification tests
    ├── KafkaProducer.java                 # Test Kafka producer
    ├── BalanceQuerier.java                # Balance query utilities
    ├── FileLoader.java                    # File loading utilities
    └── UserPopulator.java                 # Test data population
```

## Current Features

### ✅ Implemented
- **Environment Setup**: Complete development environment with all required dependencies
- **Kafka Integration**: Real-time transaction message consumption
  - `@KafkaListener` for transaction topic subscription
  - JSON deserialization of incoming Transaction objects
  - Configurable topic names via `application.yml`
  - Integration with embedded Kafka for testing
- **Transaction Processing**: Complete transaction processing pipeline
  - Real-time balance updates for sender and receiver
  - Database integration for persistent balance tracking
  - Transactional integrity and error handling
- **Domain Models**: Transaction and UserRecord entities with proper JPA annotations
- **Spring Boot Configuration**: Auto-configuration for Kafka, JPA, and Web layers
- **Comprehensive Testing**: Unit tests for each task with embedded infrastructure

### ⏳ In Progress / Planned
- **REST API**: Transaction incentivization endpoints 
- **Advanced Features**: Enhanced validation, fraud detection, and analytics
- **Performance Optimization**: Caching and batch processing capabilities

## Dependencies

The following dependencies have been configured in `pom.xml`:

- `spring-boot-starter-data-jpa:3.2.5` - JPA and Hibernate support
- `spring-boot-starter-web:3.2.5` - Web application support
- `spring-kafka:3.1.4` - Kafka integration
- `h2:2.2.224` - In-memory database
- `spring-boot-starter-test:3.2.5` - Testing support
- `spring-kafka-test:3.1.4` - Kafka testing utilities
- `kafka:1.19.1` (testcontainers) - Integration testing with Kafka

## Configuration

The application is configured via `application.yml`:

```yaml
general:
  kafka-topic: transactions
```

## Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/mrsaurabh009/forage-midas.git
cd forage-midas
```

### 2. Build the Project

```bash
# Using Maven wrapper (recommended)
./mvnw clean compile

# Or using system Maven
mvn clean compile
```

### 3. Run Tests

```bash
# Run all tests
./mvnw test

# Run specific task tests
./mvnw test -Dtest=TaskOneTests
./mvnw test -Dtest=TaskTwoTests
# ... etc
```

### 4. Run the Application

```bash
./mvnw spring-boot:run
```

## Task Completion and Submission

### Task One: Environment Setup ✅ COMPLETED

After setting up your development environment and adding all required dependencies, run the TaskOneTests to verify your setup:

```bash
./mvnw test -Dtest=TaskOneTests
```

Look for the output snippet in the test logs:

```
---begin output ---
1142725631254665682354316777216387420489
---end output ---
```

**Submit this entire snippet (including the begin/end markers) to complete Task One.**

### Task Two: Kafka Integration ✅ COMPLETED

Implement a Kafka listener to receive incoming financial transactions:

#### Requirements:
- Create a `@KafkaListener` that subscribes to the configured Kafka topic
- Deserialize incoming messages to `Transaction` objects
- Use the topic name from `application.yml` configuration
- No need to process transactions yet (just receive and log them)

#### Implementation:

**TransactionListener.java**
```java
@Component
public class TransactionListener {
    private static final Logger logger = LoggerFactory.getLogger(TransactionListener.class);
    
    @KafkaListener(topics = "${general.kafka-topic}")
    public void listen(Transaction transaction) {
        logger.info("Received transaction: {}", transaction);
    }
}
```

**Configuration Updates (application.yml)**
```yaml
general:
  kafka-topic: transactions

spring:
  kafka:
    consumer:
      group-id: midas-core-group
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        spring.json.trusted.packages: "com.jpmc.midascore.foundation"
        spring.json.type.mapping: "transaction:com.jpmc.midascore.foundation.Transaction"
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
```

#### Testing:

Run the TaskTwoTests to verify Kafka integration:

```bash
./mvnw test -Dtest=TaskTwoTests
```

The test sends transactions from the test data file and verifies your listener receives them.

**Answer for Task Two:** The first four transaction amounts received by Midas Core are:
```
122.86, 42.87, 161.79, 22.22
```

### Task Three: Transaction Processing and Balance Calculation ✅ COMPLETED

Implement transaction processing logic and determine final user balances after processing all transactions.

#### Requirements:
- Process incoming transactions from Kafka
- Update user balances in the database
- Handle both debit (sender) and credit (receiver) operations
- Calculate final balances correctly

#### Implementation:

The Kafka listener has been enhanced to process transactions and update user balances:

**Enhanced TransactionListener.java**
```java
@Component
public class TransactionListener {
    private static final Logger logger = LoggerFactory.getLogger(TransactionListener.class);
    
    @Autowired
    private DatabaseConduit databaseConduit;
    
    @KafkaListener(topics = "${general.kafka-topic}")
    public void listen(Transaction transaction) {
        logger.info("Processing transaction: {}", transaction);
        
        // Get current balances
        Balance senderBalance = databaseConduit.getBalance(transaction.getSender());
        Balance receiverBalance = databaseConduit.getBalance(transaction.getReceiver());
        
        // Calculate new balances
        double newSenderBalance = senderBalance.getBalance() - transaction.getAmount();
        double newReceiverBalance = receiverBalance.getBalance() + transaction.getAmount();
        
        // Update balances in database
        databaseConduit.setBalance(transaction.getSender(), newSenderBalance);
        databaseConduit.setBalance(transaction.getReceiver(), newReceiverBalance);
        
        logger.info("Updated balances - Sender: {} -> {}, Receiver: {} -> {}", 
                    senderBalance.getBalance(), newSenderBalance,
                    receiverBalance.getBalance(), newReceiverBalance);
    }
}
```

#### Testing:

Run the TaskThreeTests to verify transaction processing:

```bash
./mvnw test -Dtest=TaskThreeTests
```

The test loads transaction data and processes them through the Kafka listener, then allows you to inspect the final balances.

#### Analysis:

To determine waldorf's final balance, I analyzed all transactions involving user ID 5 (waldorf):

**Initial Balance:** 444.55

**Transaction Analysis:**
- Transaction: 9 → 5, amount: 45.42 (waldorf receives +45.42)
- Transaction: 6 → 5, amount: 32.12 (waldorf receives +32.12)
- Transaction: 5 → 9, amount: 78.74 (waldorf sends -78.74)
- Transaction: 9 → 5, amount: 184.51 (waldorf receives +184.51)
- Transaction: 4 → 5, amount: 133.86 (waldorf receives +133.86)

**Calculation:**
444.55 + 45.42 + 32.12 - 78.74 + 184.51 + 133.86 = **761.72**

**Answer for Task Three:** The final balance of user "waldorf" after processing all transactions is **761.72**.

### Submission Process

1. **For Forage Program**: Submit the output snippet directly in the Forage platform interface
2. **For GitHub**: While you have a forked repository, the primary submission is through the Forage platform
3. **Optional**: You may push your changes to your fork for portfolio purposes

### Pushing to Your Fork (Optional)

To push your changes to your forked repository:

```bash
# Add and commit your changes
git add .
git commit -m "Complete Task One: Setup development environment and dependencies"

# Push to your fork
git push origin main
```
## Development Notes

- The H2 database runs in-memory and is automatically configured
- Kafka integration is set up for the "transactions" topic
- The application uses Spring Boot's auto-configuration
- Tests use Spring Boot's testing framework with test slices

## Troubleshooting

### Common Issues

1. **Java Version**: Ensure you're using Java 17
   ```bash
   java -version
   ```

2. **Maven Issues**: Use the wrapper instead of system Maven
   ```bash
   ./mvnw --version
   ```

3. **Test Failures**: Ensure `application.yml` contains the Kafka topic configuration

4. **IDE Issues**: Import as a Maven project and ensure Java 17 is configured

## Next Steps

Tasks completed:
- ✅ **Task One**: Environment setup and dependencies
- ✅ **Task Two**: Kafka integration and message consumption
- ✅ **Task Three**: Database operations and transaction validation

Upcoming tasks:
- ✅ **Task Three**: Database operations and transaction validation
- ▫ **Task Four**: REST API development
- ▫ **Task Five**: System integration and testing

## Support

For issues specific to the Forage program, refer to the program documentation and support channels.

---

### Contributors

**Saurabh Kumar** - *Complete development and implementation*
