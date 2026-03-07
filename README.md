\# BDM (Big Data Management)



This repository is part of the first phase of the \*\*BDM 25-26\*\* project, focused on designing a \*\*big data processing pipeline\*\* before implementation. The goal is to define a high-level architecture showing how \*\*structured, semi-structured, and unstructured data\*\* flows from ingestion (e.g., CSV files, JSON APIs, images) into a landing zone (MinIO/S3), through processing and transformation stages (using Spark and Delta Lake), and finally to exploitation or consumption stages for analysis or downstream applications. The project contextualizes the problem in the \*\*climate and environmental domain\*\*, aiming to provide insights from historical and live datasets while demonstrating robust data management. Although some components, like Trusted and Exploitation Zones, are currently black boxes, the design lays a solid blueprint for future development, including handling large datasets, metadata management, and potential integration with machine learning workflows.



---



\### Start Docker Containers



Use the following command to deploy MinIO, ..., containers.



```bash

docker compose up -d

```

---

