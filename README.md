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

### Task One: Environment Setup

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

After completing Task One, you'll work on:
- Task Two: Database integration and JPA setup
- Task Three: Kafka message processing
- Task Four: REST API development
- Task Five: System integration and testing

## Support

For issues specific to the Forage program, refer to the program documentation and support channels.
