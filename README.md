# 🐺 Minnesota Timberwolves Data Pipeline

A production-ready data pipeline that extracts NBA Minnesota Timberwolves game and player statistics using Apache Airflow and loads them into Snowflake for analytics.

## 🏀 Overview

This pipeline automatically extracts:
- **Game data**: All 82 regular season games for the Minnesota Timberwolves
- **Player statistics**: Detailed box score stats for every player in each game
- **Historical data**: Complete 2024-25 NBA season data

**Data Sources**: NBA Stats API via `nba_api` Python package  
**Orchestration**: Apache Airflow (Astronomer Cloud)  
**Data Warehouse**: Snowflake  
**Schedule**: Daily at 6:00 AM ET (11:00 UTC)

## 📊 Data Output

### Games Table (`NBA.RAW.GAMES`)
- 82 games from 2024-25 season
- Game metadata, scores, team performance metrics
- Fields: `game_id`, `game_date`, `matchup`, `result`, `team_points`, etc.

### Player Stats Table (`NBA.RAW.PLAYER_STATS`) 
- 2,132+ player performance records
- Detailed box score statistics for every game
- Fields: `player_name`, `points`, `rebounds`, `assists`, `minutes`, etc.

## 🚀 Quick Start

### Prerequisites
- [Astronomer CLI](https://docs.astronomer.io/astro/cli/install-cli)
- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- Snowflake account with:
  - Database: `NBA`
  - Schema: `RAW` 
  - Tables: `GAMES`, `PLAYER_STATS`
  - Stage: `CSV_STAGE`
  - File Format: `CSV_FMT`

### Local Development

1. **Clone the repository**
   ```bash
   git clone https://github.com/xWesty1/Wolves-Pipeline.git
   cd Wolves-Pipeline
   ```

2. **Start local Airflow**
   ```bash
   astro dev start
   ```

3. **Access Airflow UI**
   - URL: http://localhost:8080
   - Username: `admin`
   - Password: `admin`

4. **Configure Snowflake connection**
   - Connection ID: `snowflake_default`
   - Connection Type: `Generic`
   - Host: `your-org-your-account` (e.g., `bcuhkxz-yc43329`)
   - Login: `your-username`
   - Password: `your-password`

### Production Deployment

1. **Deploy to Astronomer Cloud**
   ```bash
   astro login
   astro deploy your-deployment-id
   ```

2. **Configure environment variables**
   ```
   TEAM_ID=1610612750    # Minnesota Timberwolves
   SEASON=2024-25        # NBA season
   ```

## 📁 Project Structure

```
wolves_pipe/
├── dags/
│   └── wolves_pipeline.py           # Main Airflow DAG
├── include/
│   └── nba-twolves-pipeline/
│       ├── wolves_extractor.py      # NBA data extraction script
│       ├── requirements.txt         # Python dependencies
│       └── data/raw/YYYYMMDD/       # Generated CSV files
├── requirements.txt                 # Main project dependencies
├── Dockerfile                       # Container configuration
└── README.md                       # This file
```

## 🔧 How It Works

### 1. Data Extraction
- Uses `nba_api` to fetch Timberwolves game log and box scores
- Extracts data for specified `TEAM_ID` and `SEASON`
- Generates CSV files in `data/raw/YYYYMMDD/` format

### 2. Data Loading
- Connects to Snowflake using configured credentials
- Stages CSV files using `PUT` commands to `@CSV_STAGE`
- Loads data using `COPY INTO` commands with error handling

### 3. Error Handling
- Retry logic for NBA API timeouts
- File validation before Snowflake operations  
- Comprehensive logging and error reporting

## 📅 Schedule

**Production Schedule**: Daily at 6:00 AM ET (11:00 UTC)  
**Start Date**: June 1, 2025  
**Catchup**: Disabled  
**Retries**: 1 retry with 5-minute delay

## 🛠️ Configuration

### Environment Variables
```python
TEAM_ID="1610612750"    # Minnesota Timberwolves NBA team ID
SEASON="2024-25"        # NBA season in YYYY-YY format
```

### Snowflake Setup
```sql
-- Create database and schema
CREATE DATABASE NBA;
CREATE SCHEMA NBA.RAW;

-- Create tables (adjust data types as needed)
CREATE TABLE NBA.RAW.GAMES (...);
CREATE TABLE NBA.RAW.PLAYER_STATS (...);

-- Create stage and file format
CREATE STAGE NBA.RAW.CSV_STAGE;
CREATE FILE FORMAT NBA.RAW.CSV_FMT TYPE = 'CSV' FIELD_DELIMITER = ',';
```

## 📈 Data Pipeline Flow

```
NBA Stats API → Python Extractor → CSV Files → Snowflake Stage → Snowflake Tables
```

1. **Extract**: Fetch game logs and box scores from NBA API
2. **Transform**: Clean and structure data into CSV format
3. **Stage**: Upload CSV files to Snowflake internal stage
4. **Load**: Copy data from stage into final tables

## 🔍 Monitoring

- **Airflow UI**: Monitor DAG runs, task status, and logs
- **Astronomer Cloud**: View deployment health and metrics
- **Snowflake**: Query loaded data and verify completeness

## 🐛 Troubleshooting

### Common Issues

**NBA API Timeouts**
- Pipeline includes retry logic with exponential backoff
- Off-season periods may have reduced API availability

**Snowflake Connection Errors**
- Verify account identifier format: `organization-account`
- Check credentials and network connectivity

**File Not Found Errors**
- Ensure NBA API extraction completed successfully
- Check logs for data extraction failures

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is open source and available under the [MIT License](LICENSE).

## 📧 Contact

**Project Creator**: [@xWesty1](https://github.com/xWesty1)  
**Repository**: [Wolves-Pipeline](https://github.com/xWesty1/Wolves-Pipeline)

---

*Built with ❤️ for Minnesota Timberwolves analytics*
