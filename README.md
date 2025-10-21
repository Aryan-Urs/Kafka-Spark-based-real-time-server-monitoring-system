# 🧠 Spark–Kafka Server Monitoring

This project implements a **Kafka–Spark batch processing system** to monitor server metrics in real time.  
It consumes **CPU, Memory, Network, and Disk** data from Kafka topics, aggregates them in **sliding windows**, and raises alerts when usage thresholds are exceeded.

---

## ⚙️ Project Structure
│
├── consumer1.py # Reads CPU + MEM data from Kafka and saves raw CSVs
├── sparkjob1.py # Processes CPU + MEM CSVs → team_87_CPU_MEM.csv
│
├── consumer2.py # Reads NET + DISK data from Kafka and saves raw CSVs
├── sparkjob2.py # Processes NET + DISK CSVs → team_87_NET_DISK.csv
│
├── thresholds.txt # Threshold values for alerts
├── README.md # Project documentation (this file)
└── *.csv # Generated output files



---

## 🧩 Data Flow Diagram

```text
┌───────────────┐       ┌─────────────┐       ┌─────────────┐       ┌──────────────┐
│ Kafka Topics  │──────▶│ Consumers   │──────▶│ Raw CSVs    │──────▶│ Spark Jobs   │
│ (cpu, mem,    │       │ (consumer1, │       │ (cpu_data,  │       │ (sparkjob1,  │
│ net, disk)    │       │ consumer2)  │       │ mem_data...)│       │ sparkjob2)   │
└───────────────┘       └─────────────┘       └─────────────┘       └──────┬───────┘
                                                                           ▼
                                                                   Processed CSV Reports
                                                                   (team_87_CPU_MEM.csv,
                                                                    team_87_NET_DISK.csv)
