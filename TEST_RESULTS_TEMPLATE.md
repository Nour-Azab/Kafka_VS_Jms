# Lab 4: Test Results & Data Collection Template

## Instructions for Data Population

After running the tests with the provided code, populate this section with actual measurements.

---

## Part A: Performance Comparison Results

### A1: Response Time Measurements

#### JMS Producer Response Time
After running 10,000 producer sends:

| Run # | Median (ms) | Min (ms) | Max (ms) | Notes |
|-------|------------|---------|---------|-------|
| 1 | [POPULATE] | [POPULATE] | [POPULATE] | |
| 2 | [POPULATE] | [POPULATE] | [POPULATE] | |
| 3 | [POPULATE] | [POPULATE] | [POPULATE] | |
| 4 | [POPULATE] | [POPULATE] | [POPULATE] | |
| 5 | [POPULATE] | [POPULATE] | [POPULATE] | |

**Average Median Response Time (JMS Producer):** `[CALCULATE AVERAGE]` ms

#### Kafka Producer Response Time  
After running 10,000 producer sends:

| Run # | Median (ms) | Min (ms) | Max (ms) | Notes |
|-------|------------|---------|---------|-------|
| 1 | [POPULATE] | [POPULATE] | [POPULATE] | |
| 2 | [POPULATE] | [POPULATE] | [POPULATE] | |
| 3 | [POPULATE] | [POPULATE] | [POPULATE] | |
| 4 | [POPULATE] | [POPULATE] | [POPULATE] | |
| 5 | [POPULATE] | [POPULATE] | [POPULATE] | |

**Average Median Response Time (Kafka Producer):** `[CALCULATE AVERAGE]` ms

#### JMS Consumer Response Time

| Run # | Median (ms) | Min (ms) | Max (ms) | Messages Received |
|-------|------------|---------|---------|------------------|
| 1 | [POPULATE] | [POPULATE] | [POPULATE] | |
| 2 | [POPULATE] | [POPULATE] | [POPULATE] | |
| 3 | [POPULATE] | [POPULATE] | [POPULATE] | |
| 4 | [POPULATE] | [POPULATE] | [POPULATE] | |
| 5 | [POPULATE] | [POPULATE] | [POPULATE] | |

**Average Median Response Time (JMS Consumer):** `[CALCULATE AVERAGE]` ms

#### Kafka Consumer Response Time

| Run # | Median (ms) | Min (ms) | Max (ms) | Messages Received |
|-------|------------|---------|---------|------------------|
| 1 | [POPULATE] | [POPULATE] | [POPULATE] | |
| 2 | [POPULATE] | [POPULATE] | [POPULATE] | |
| 3 | [POPULATE] | [POPULATE] | [POPULATE] | |
| 4 | [POPULATE] | [POPULATE] | [POPULATE] | |
| 5 | [POPULATE] | [POPULATE] | [POPULATE] | |

**Average Median Response Time (Kafka Consumer):** `[CALCULATE AVERAGE]` ms

---

### A2: Latency Measurements (10K Messages)

#### JMS Latency Percentiles

| Percentile | Latency (ms) |
|-----------|--------------|
| 50th (Median) | [POPULATE] |
| 75th | [POPULATE] |
| 95th | [POPULATE] |
| 99th | [POPULATE] |
| Max | [POPULATE] |

#### Kafka Latency Percentiles

| Percentile | Latency (ms) |
|-----------|--------------|
| 50th (Median) | [POPULATE] |
| 75th | [POPULATE] |
| 95th | [POPULATE] |
| 99th | [POPULATE] |
| Max | [POPULATE] |

---

### A3: Throughput Measurements

#### JMS Producer Maximum Throughput Test

Test Configuration:
- Start with: 1,000 msg/sec
- Increase by: 50% each iteration
- Test duration: 1 second per throughput level
- Payload size: 1 KB

| Throughput (msg/s) | Sent | Delivered | Failed | Status |
|------------------|------|-----------|--------|--------|
| 1,000 | [POPULATE] | [POPULATE] | [POPULATE] | [PASS/FAIL] |
| 1,500 | [POPULATE] | [POPULATE] | [POPULATE] | [PASS/FAIL] |
| 2,250 | [POPULATE] | [POPULATE] | [POPULATE] | [PASS/FAIL] |
| 3,375 | [POPULATE] | [POPULATE] | [POPULATE] | [PASS/FAIL] |
| 5,062 | [POPULATE] | [POPULATE] | [POPULATE] | [PASS/FAIL] |
| 7,593 | [POPULATE] | [POPULATE] | [POPULATE] | [PASS/FAIL] |
| 11,390 | [POPULATE] | [POPULATE] | [POPULATE] | [PASS/FAIL] |
| 17,085 | [POPULATE] | [POPULATE] | [POPULATE] | [PASS/FAIL] |

**Maximum Sustainable Throughput (JMS Producer):** `[RECORD]` msg/sec

#### Kafka Producer Maximum Throughput Test

Using: `kafka-producer-perf-test.sh`

```bash
Command used:
./kafka-producer-perf-test.sh \
--topic lab-topic \
--num-records 1000000 \
--record-size 1000 \
--throughput 1000000 \
--producer-props bootstrap.servers=localhost:9092 acks=all
```

**Output:**
```
[PASTE COMMAND OUTPUT HERE]
```

**Maximum Sustainable Throughput (Kafka Producer):** `[RECORD]` msg/sec

#### JMS Consumer Maximum Throughput Test

| Throughput (msg/s) | Received | Missing | Status |
|------------------|----------|---------|--------|
| 5,000 | [POPULATE] | [POPULATE] | [PASS/FAIL] |
| 7,500 | [POPULATE] | [POPULATE] | [PASS/FAIL] |
| 10,000 | [POPULATE] | [POPULATE] | [PASS/FAIL] |
| 15,000 | [POPULATE] | [POPULATE] | [PASS/FAIL] |

**Maximum Sustainable Throughput (JMS Consumer):** `[RECORD]` msg/sec

#### Kafka Consumer Maximum Throughput Test

Using: `kafka-consumer-perf-test.sh`

```bash
Command used:
./kafka-consumer-perf-test.sh \
--broker-list localhost:9092 \
--topic lab-topic \
--messages 1000000 \
--threads 1
```

**Output:**
```
[PASTE COMMAND OUTPUT HERE]
```

**Maximum Sustainable Throughput (Kafka Consumer):** `[RECORD]` msg/sec

---

### A4: Performance Summary Table

| Metric | JMS | Kafka | Winner |
|--------|-----|-------|--------|
| Producer Response Time Median | [POPULATE] ms | [POPULATE] ms | [AUTO-CALC] |
| Consumer Response Time Median | [POPULATE] ms | [POPULATE] ms | [AUTO-CALC] |
| P50 Latency | [POPULATE] ms | [POPULATE] ms | [AUTO-CALC] |
| P95 Latency | [POPULATE] ms | [POPULATE] ms | [AUTO-CALC] |
| P99 Latency | [POPULATE] ms | [POPULATE] ms | [AUTO-CALC] |
| Producer Max Throughput | [POPULATE] msg/s | [POPULATE] msg/s | [AUTO-CALC] |
| Consumer Max Throughput | [POPULATE] msg/s | [POPULATE] msg/s | [AUTO-CALC] |

---

## Part B: Usability Assessment

### B1: Setup Time & Complexity

#### JMS Setup
**Total Time:** [POPULATE] minutes

Steps:
1. Download ActiveMQ: [TIME] minutes
2. Extract & Configure: [TIME] minutes  
3. Start Broker: [TIME] minutes
4. Verify Installation: [TIME] minutes
5. Create Maven Project: [TIME] minutes
6. Add Dependencies: [TIME] minutes
7. Write & Test Code: [TIME] minutes

**Total Setup Time:** [POPULATE] minutes
**Complexity Score:** [1-10]
**Issues Encountered:**
- [LIST ANY ISSUES]

#### Kafka Setup
**Total Time:** [POPULATE] minutes

Steps:
1. Download Kafka: [TIME] minutes
2. Extract & Configure: [TIME] minutes
3. Start Zookeeper: [TIME] minutes
4. Start Kafka: [TIME] minutes
5. Create Topic: [TIME] minutes
6. Create Maven Project: [TIME] minutes
7. Add Dependencies: [TIME] minutes
8. Write & Test Code: [TIME] minutes

**Total Setup Time:** [POPULATE] minutes
**Complexity Score:** [1-10]
**Issues Encountered:**
- [LIST ANY ISSUES]

### B2: Code Complexity Analysis

#### JMS Code Metrics
- Total Lines (Producer Setup + Send): [COUNT]
- Total API Calls: [COUNT]
- Resource Management Classes: [COUNT]
- Configuration Parameters: [COUNT]

#### Kafka Code Metrics
- Total Lines (Producer Setup + Send): [COUNT]
- Total API Calls: [COUNT]
- Resource Management Classes: [COUNT]
- Configuration Parameters: [COUNT]

### B3: Usability Scoring

| Criterion | JMS (1-10) | Kafka (1-10) | Notes |
|-----------|-----------|-------------|-------|
| Setup Time | [SCORE] | [SCORE] | |
| Code Simplicity | [SCORE] | [SCORE] | |
| Documentation Quality | [SCORE] | [SCORE] | |
| Debugging Difficulty | [SCORE] | [SCORE] | |
| Learning Curve | [SCORE] | [SCORE] | |
| **Overall** | [AVG] | [AVG] | |

---

## Part C: Integration Research Results

### C1: Programming Language Support

**Research Method:** [Describe how you researched this]

#### JMS Language Support

| Language | Supported | Quality | Examples/Links |
|----------|-----------|---------|-----------------|
| Java | Yes | [Native/Library/Bridge] | [LINK] |
| Python | [Yes/No] | [Quality] | [LINK] |
| Node.js | [Yes/No] | [Quality] | [LINK] |
| Go | [Yes/No] | [Quality] | [LINK] |
| C# | [Yes/No] | [Quality] | [LINK] |
| Ruby | [Yes/No] | [Quality] | [LINK] |
| Rust | [Yes/No] | [Quality] | [LINK] |
| PHP | [Yes/No] | [Quality] | [LINK] |

#### Kafka Language Support

| Language | Supported | Quality | Examples/Links |
|----------|-----------|---------|-----------------|
| Java | Yes | Native | kafka-clients |
| Python | Yes | [Quality] | [LINK] |
| Node.js | Yes | [Quality] | [LINK] |
| Go | Yes | [Quality] | [LINK] |
| C# | Yes | [Quality] | [LINK] |
| Ruby | Yes | [Quality] | [LINK] |
| Rust | Yes | [Quality] | [LINK] |
| PHP | [Yes/No] | [Quality] | [LINK] |

### C2: Data Source Integrations

**Research Method:** [Describe your research approach]

#### JMS Integrations

| Category | Integration | Availability | Maturity | Source |
|----------|-----------|-------------|----------|--------|
| Databases | [List] | [Available/Custom] | [Link] | |
| Cloud Services | [List] | [Available/Custom] | [Link] | |
| Analytics | [List] | [Available/Custom] | [Link] | |
| Message Queues | [List] | [Available/Custom] | [Link] | |
| Streaming | [List] | [Available/Custom] | [Link] | |

**Total Integrations Found:** [COUNT]

#### Kafka Integrations via Kafka Connect

| Category | Integrations | Count | Status | Source |
|----------|-------------|-------|--------|--------|
| Databases | [List] | [#] | [Status] | [Link] |
| Cloud (AWS) | [List] | [#] | [Status] | [Link] |
| Cloud (Azure) | [List] | [#] | [Status] | [Link] |
| Cloud (GCP) | [List] | [#] | [Status] | [Link] |
| Analytics | [List] | [#] | [Status] | [Link] |
| Message Queues | [List] | [#] | [Status] | [Link] |
| Streaming | [List] | [#] | [Status] | [Link] |
| Data Warehouses | [List] | [#] | [Status] | [Link] |
| Data Lakes | [List] | [#] | [Status] | [Link] |

**Total Integrations Found:** [COUNT]

### C3: Integration Ecosystem Score

| Aspect | JMS Score | Kafka Score | Notes |
|--------|-----------|-------------|-------|
| Language Coverage | [SCORE] | [SCORE] | |
| Data Source Integrations | [SCORE] | [SCORE] | |
| Connector Quality | [SCORE] | [SCORE] | |
| Cloud Platform Support | [SCORE] | [SCORE] | |
| Community Contributions | [SCORE] | [SCORE] | |
| **Overall Integration** | [AVG] | [AVG] | |

---

## Part D: Summary & Recommendations

### D1: Performance Winner: [SELECT]
**Justification:** [EXPLAIN]

### D2: Usability Winner: [SELECT]  
**Justification:** [EXPLAIN]

### D3: Integration Winner: [SELECT]
**Justification:** [EXPLAIN]

### D4: Overall Recommendation: [KAFKA / JMS / CONDITIONAL]

**Recommendation Summary:**
[Write 3-5 sentences explaining your final choice]

**Key Deciding Factor:**
[What was the most important factor in your decision?]

---

## References Used

1. [Reference 1 - URL/Title]
2. [Reference 2 - URL/Title]
3. [Reference 3 - URL/Title]
4. [Reference 4 - URL/Title]
5. [Reference 5 - URL/Title]

---

## Appendix: Commands Used

### JMS Testing Commands
```bash
# [Commands used for JMS testing]
```

### Kafka Testing Commands
```bash
# [Commands used for Kafka testing]
```

---

**Report Completed:** [DATE]
**Team Members:** [List all team members]
**Email:** [Contact information]
