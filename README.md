# BTC Price Prediction

This project demonstrates a simple Kafka consumer and producer setup for real-time BTC price prediction using a linear regression model.

## Introduction

This project consists of two main components:
- **Kafka Consumer**: Retrieves real-time BTC price data from a Kafka topic, uses a pre-trained linear regression model to predict the closing price, and calculates prediction accuracy.
- **Kafka Producer**: Reads historical BTC price data from a CSV file, converts it to JSON format, and produces messages to a Kafka topic.


## Requirements

Ensure you have the following dependencies installed before running the project:
- Python 3.x
- confluent_kafka
- pandas
- scikit-learn

You can install the required Python packages using the following command:

```bash
pip install confluent-kafka pandas scikit-learn
```

## Usage

1. **Start the Kafka Producer:**
open new terminal window and run this code.
```bash
python Producer.py
```

2. **Start the Spark Consumer:**
open new terminal window and run this code.
```bash
python Consumer.py
```
