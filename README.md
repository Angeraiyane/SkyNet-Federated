# SkyNet: Federated Intelligence and Real-Time Air Surveillance



**SkyNet** is a distributed Big Data platform designed for real-time ingestion, processing, and predictive analysis of global aircraft flight states. By leveraging the **Federated Learning** paradigm, SkyNet processes massive **ADS-B** aviation data streams locally at the edge. This strategy ensures zero-latency trajectory predictions, drastically reduces network bandwidth requirements, and preserves data sovereignty across international borders.



This project was developed as part of the Master's program in AI \& Data Computing at the Faculty of Sciences and Techniques (FST), Sultan Moulay Slimane University (USMS), Beni Mellal.



\---

* #### **System Architecture**



The decentralized infrastructure is built around three core pillars:

1\. **High-Frequency Ingestion Pipeline (Apache Kafka \& ZooKeeper):** A robust distributed streaming backbone capable of absorbing raw OpenSky state vectors and syncing federated nodes via an advanced replication mechanism (Replication Factor = 3). (here we just absorb 100 vectors per 2 minutes, so feel free to change that according to your computer characterictics)

2\. **Edge Intelligence (Federated Clients):** Autonomous edge agents simulating key geographical nodes (**FST\_BM** tracking Africa/Asia/Americas and **TOGO** tracking Europe/Oceania). These nodes continuously train a local `SGDRegressor` model on incoming live streams using incremental online learning.

3\. **Supervision \& Analytics Dashboard (Streamlit):** A "Near Real-Time" command-center interface displaying live radar feeds, AI tracking metrics, and historical day-over-day (D vs D-1) traffic stability.



###### &#x20; **Data Flows and Topic Replication Strategy**



\* **flight\_data\_raw**: Stream of raw ADS-B positions (Leader: Morocco Broker).



\* **model\_updates**: Transmission of federated mathematical weights (y = w.x + b) calculated at the edge (Leader: Togo Broker).



\* **global\_model\_results**: Aggregation and broadcasting of global metrics and analytics (Leader: Cloud/Central Broker).



\---



* #### **Tech Stack**



* **Big Data Infrastructure:** Apache Kafka, Apache ZooKeeper
* **Artificial Intelligence:** Scikit-Learn (SGDRegressor for online/incremental machine learning)
* **Geo-AI Graph Database:** Neo4j (Trajectory modeling and spatial enrichment using a local **Nominatim**`)
* **Visualization Engine:** Streamlit, Plotly Express
* E**nvironment \& Tools:** Python 3.x, Git, MINGW64 / Git Bash



\---



* #### **Project Structure**



**SkyNet-Federated/**

│

├── **producers/**

│   └── **flight\_producer.py**      # Connects to OpenSky and publishes raw streams to Kafka

│

├── **consumers/**

│   ├── **federated\_client.py**     # Edge agents (FST\_BM / TOGO) - Local SGD training

│   └── **federated\_server.py**     # Central server aggregating federated intelligence weights

│

├── **database/**

│   └── **neo4j\_connector.py**      # Offline reverse geocoding \& Neo4j graph database injection

│

├── **design/**

│   └── dashboard.py            # Streamlit UI dashboard (Radar map, D vs D-1 stats, Metrics)

│

├── **.gitignore**                  # Prevents committing Python caches, logs, and local temporary files

└── **README.md**                   # Project documentation







## **🚀 Getting Started**

**Follow these steps in strict chronological order to boot up the entire SkyNet ecosystem. Open a separate terminal window for each command:**



1. ###### **Fire Up the Distributed Infrastructure**

&#x20; **Start the coordination manager first, followed by the message brokers :**



|**Bash<br /><br />**# Terminal 1: Apache ZooKeeper<br />zookeeper-server-start.sh config/zookeeper.properties<br /><br /># Terminal 2: Apache Kafka Broker (Wait 10 seconds after ZooKeeper initializes)<br />kafka-server-start.sh config/server.properties<br />|
|-|





###### **2. Launch the Data ingestion Pipeline**

&#x20; **Run the Producer script to start feeding live ADS-B flight vectors into cluster :** 



| **Bash<br /><br />**# Terminal 3<br />python producers/flight\_producer.py<br />|
|-|





###### **3. Activate the Federated Learning Loop**

&#x20; **Boot up the central parameter aggregator, the deploy your geographic edge clients to the network :**



| **Bash<br /><br />**# Terminal 4: Central Aggregator Server<br />python consumers/federated\_server.py<br /><br /># Terminal 5: Morocco Edge Node (FST Beni Mellal)<br />python consumers/federated\_client.py --name FST\_BM --continents AFRICA ASIA AMERICA<br /><br /># Terminal 6: Togo Edge Node<br />python consumers/federated\_client.py --name TOGO --continents EUROPE OCEANIA<br />|
|-|







###### **4. Initialize the Geo-AI knowledge Graph**

&#x20; **Lauch the Neo4j connector to automatically map positions into hierchical geographic structures using lightning-fast country ISO codes :**



| **Bash<br /><br />**# Terminal 7<br />python database/neo4j\_connector.py<br />|
|-|





###### **5. Open the Monitoring Dashboard**

&#x20; **Run the frontend monitoring interface (autimatically refreshes every \~3.6 seconds to ensure data fluidity without overloading the UI):**



|**Bash<br /><br />**#Terminal 8<br />streamlit run design/dashboard.py<br />|
|-|





## **\~ Future PErspectives (Skynet 2.0)**







* ###### **Predictive Airborne Collicion Avoidance (Next-Gen TCAS ) :**

&#x20;Utilizing **T+10T** minutes trajectory projections to automatically analyse convergence vectors between multiple aircraft at identical altitudes, raising poractive safety alerts.



* ###### **Throughput Anomaly Detection :** 

&#x20;Monitoring the byte-size and message frequencies of local historical .json against an established Baseline to flag sudden traffic spikes (indicative of sensor failures or malicious network flooding).



* ###### **Stateful Disaster Recovery :** 

&#x20; Implementing rigorous local state persistence allowing edge nodes to instantly recover their last trained weights from local files in the event of a sudden power grid or connection Failure.



* ###### **Complex temporal AI Models :** 

&#x20; Upgrading the linear model to Deep Learning **LSTM** (Nong Short-Term Memory) recurrent networks to better capture highly non-linear maneuvers (sharp turns, holding patterns) and stabilize prediction accuracy.





###### ***Developed with dedication as part of Big Data \& Ai Reasearch initiatives (2026)***

