# Flight Delay Analytics Pipeline

A production-ready data engineering pipeline for analyzing US flight delay data using modern data stack: **Docker**, **PostgreSQL**, **Apache Airflow**, and **dbt**.

## 🏗️ Architecture

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐     ┌──────────────┐
│   Ingest    │────▶│  Transform   │────▶│  Aggregate  │────▶│  Analytics   │
│  (Python)   │     │    (dbt)     │     │   (dbt)     │     │  Dashboard   │
└─────────────┘     └──────────────┘     └─────────────┘     └──────────────┘
       │                    │                     │
       ▼                    ▼                     ▼
┌──────────────────────────────────────────────────────────┐
│                    PostgreSQL Database                    │
│  ┌─────────┐    ┌──────────┐    ┌───────────────────┐   │
│  │   raw   │───▶│ staging  │───▶│    analytics      │   │
│  └─────────┘    └──────────┘    └───────────────────┘   │
└──────────────────────────────────────────────────────────┘
                         │
                         ▼
              ┌──────────────────┐
              │  Airflow (DAG)   │
              │   Orchestration  │
              └──────────────────┘
```

### Data Flow
1. **Ingest**: Raw flight data loaded into `raw.flights` table
2. **Transform**: Data cleaned and deduplicated via dbt staging models
3. **Aggregate**: Analytics tables created for KPIs and reporting
4. **Orchestration**: Airflow DAG manages the entire pipeline

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- Git

### Setup

1. **Clone the repository**
```bash
git clone <repository-url>
cd flight-delay-analytics-pipeline
```

2. **Start the services**
```bash
docker-compose up -d
```

3. **Access Airflow UI**
- URL: http://localhost:8080
- Username: `admin`
- Password: `admin`

4. **Trigger the pipeline**
- Navigate to DAGs
- Enable `flight_delay_pipeline`
- Trigger manually or wait for scheduled run (daily at 2 AM)

## 📁 Project Structure

```
.
├── dags/                          # Airflow DAGs
│   └── flight_delay_pipeline.py  # Main ETL orchestration
├── pipeline/                      # Python ETL modules
│   ├── ingest.py                 # Data ingestion
│   ├── transform.py              # Data transformation
│   └── aggregate.py              # Data aggregation
├── dbt_project/                  # dbt transformations
│   ├── models/
│   │   ├── staging/              # Staging models
│   │   └── analytics/            # Analytics models
│   └── dbt_project.yml
├── sql/                          # SQL scripts
│   ├── init.sql                  # Database initialization
│   └── kpi_queries.sql           # KPI queries
├── docker-compose.yml            # Service orchestration
├── Dockerfile                    # Application container
└── requirements.txt              # Python dependencies
```

## 🔧 Configuration

### Environment Variables
Copy `.env.example` to `.env` and adjust as needed:
```bash
cp .env.example .env
```

### Database Schemas
- **raw**: Ingested raw data
- **staging**: Cleaned and validated data
- **analytics**: Aggregated metrics and KPIs

## 📊 Analytics Tables

### `analytics.daily_airline_stats`
Daily performance metrics by airline:
- Total flights
- Cancellation rate
- Average delays
- On-time percentage

### `analytics.route_performance`
Route-level analysis:
- Flight volume
- Average delays
- On-time performance

## 🧪 Running dbt Models

```bash
# Enter the dbt container
docker-compose exec airflow-webserver bash

# Navigate to dbt project
cd /opt/airflow/dbt_project

# Run models
dbt run

# Run tests
dbt test
```

## 📈 Monitoring

- **Airflow UI**: http://localhost:8080
- **Postgres**: localhost:5432
  - Database: `airflow`
  - User: `airflow`
  - Password: `airflow`

## 🛠️ Development

### Adding New Models
1. Create SQL file in `dbt_project/models/`
2. Define in `schema.yml`
3. Run `dbt run` to materialize

### Modifying Pipeline
1. Edit Python modules in `pipeline/`
2. Update DAG in `dags/flight_delay_pipeline.py`
3. Rebuild container: `docker-compose up -d --build`

## 📝 License

See LICENSE file for details.
