# BD25_Project_A7_C

## Data Pipeline with Kafka
This project is a data pipeline that reads radiation data from a CSV file, processes it, and sends it to a Kafka topic. It uses Docker Compose to orchestrate the services, including a Kafka broker and a data provider service. 

---

## Project Structure

```
bd25_project_a7_c/
├── docker-compose.yaml
├── data_provider/
│   ├── Dockerfile
│   ├── config.ini
│   ├── data_provider.py
│   ├── requirements.txt
│   └── safecast_data/
│       └── measurements-out.csv
```

- **`docker-compose.yaml`**: Defines the services for the project.
- **`data_provider/`**: Contains the data provider service code and configuration.
- **`safecast_data/`**: Directory where the dataset (`measurements-out.csv`) should be placed.

---

## How to Run the Application

### 1. Place the Dataset

Ensure the dataset file (`measurements-out.csv`) is placed in the following directory:

```
data_provider/safecast_data/measurements-out.csv
```

### 2. Start the Application

Run the following command to start the services:

```bash
docker-compose up
```

This will start:
- A Kafka broker on `localhost:9092`.
- The data provider service, which will read the dataset and send data to Kafka.

---

That's it! The application will process the dataset and stream it to the Kafka topic.