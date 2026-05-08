# Fuel Station Demand & Inventory Management Platform

Production-style analytics + operations platform built with **Python, SQL, SQLite/PostgreSQL migration scripts, Pandas, Plotly, and Streamlit**.

This project simulates a city-level fuel network and supports:
- role-based access (`fuel_user`, `station_owner`, `admin`)
- operational inventory updates
- recommendation system
- geospatial nearby search
- map-based monitoring
- SQL-heavy analytics dashboards
- ETL ingestion, batch aggregation, audit/lineage logs, and data quality checks
- processed report export to AWS S3 via `boto3`

---

## 1. Product Overview

The platform is designed as a combined:
- **Operations tool** for inventory and pricing actions
- **Analytics tool** for demand, pricing, risk, growth, revenue, and turnover analysis

Typical use cases:
1. Fuel users find the best nearby station by stock + price.
2. Station owners manage only their assigned stations and monitor risk/revenue.
3. Admins oversee the full network, create stations/users, and drive pricing + supply strategy.

---

## 2. Technology Stack

- **Backend language:** Python 3.12+
- **Database:** SQLite
- **Migration target:** PostgreSQL (`migrate_to_postgres.sql`)
- **Data handling:** Pandas
- **UI framework:** Streamlit
- **Charts:** Plotly
- **Map engine:** Folium + MarkerCluster
- **Cloud export:** AWS S3 using boto3
- **Data source options:**
  - synthetic seed generator
  - real station ingestion from OpenStreetMap (Overpass API)

---

## 3. Project Structure

- `schema.sql`  
  SQL schema for all core entities and constraints.

- `database.py`  
  Data access layer: DB connection management, CRUD operations, reusable analytics queries, role-scoped SQL.

- `data_generator.py`  
  Database seeding:
  - synthetic station/inventory/transaction generation
  - OSM-based real location ingestion
  - realistic station naming strategy
  - default user/account and owner-station mapping creation

- `utils.py`  
  Recommendation engine logic for ranking stations.

- `ingestion.py`  
  ETL loader for raw CSV/JSON/JSONL transaction files. Validates schema, nulls, fuel types, duplicates, and missing station IDs.

- `transformations.py`  
  Batch aggregation refresh for daily demand summaries, inventory snapshots, and revenue rollups.

- `quality_checks.py`  
  Data quality checks for outlier prices, negative inventory, missing station IDs, and negative transactions.

- `pipeline_audit.py`  
  Audit and lineage helpers for pipeline runs, rejected records, and quality report rows.

- `pipeline_runner.py`  
  Orchestrates ingestion, transformations, quality checks, and optional S3 export. Supports repeated interval runs to simulate scheduling.

- `s3_export.py`  
  Exports processed report tables to CSV and uploads them to S3.

- `migrate_to_postgres.sql`  
  PostgreSQL schema migration script with equivalent normalized and pipeline tables.

- `app.py`  
  Full Streamlit product UI:
  - login/signup
  - role-specific workspaces
  - dynamic controls
  - map + analytics visualizations
  - pipeline health and data quality reporting

- `requirements.txt`  
  Python dependencies.

- `assets/landing_hero.jpg`  
  Landing page image used as full-screen login background.

---

## 4. Data Model (Database Schema)

### `users`
- `user_id` (PK)
- `username` (unique)
- `password`
- `role` (`fuel_user`, `station_owner`, `admin`)
- `home_latitude`, `home_longitude`
- `created_at`

### `stations`
- `station_id` (PK)
- `name`
- `area`
- `latitude`, `longitude`

### `fuel_inventory`
- `id` (PK)
- `station_id` (FK -> stations)
- `fuel_type` (`Petrol`, `Diesel`)
- `available_liters`
- `price`
- `last_updated`
- unique constraint on (`station_id`, `fuel_type`)

### `transactions`
- `txn_id` (PK)
- `station_id` (FK -> stations)
- `fuel_type`
- `liters_sold`
- `txn_time`

### `owner_station_access`
- `id` (PK)
- `user_id` (FK -> users)
- `station_id` (FK -> stations)
- `assigned_at`
- unique constraint on (`user_id`, `station_id`)

### Data Engineering Tables
- `pipeline_runs`: run-level audit/lineage with status, timestamps, ingested/rejected counts
- `rejected_records`: raw payloads rejected during ingestion with reasons
- `daily_demand_summary`: scheduled daily station/fuel demand aggregates
- `inventory_snapshots`: point-in-time inventory and price snapshots
- `revenue_rollups`: daily estimated revenue aggregates
- `data_quality_reports`: anomaly counts and severity by quality check

---

## 5. Core Functional Modules

## 5.1 Authentication and Roles
- Login with username/password.
- Signup inside app with role selection.
- `admin` signup protected via `ADMIN_SIGNUP_CODE`.
- Station owner signup supports direct station assignment.
- Role-based workspace routing after login.

## 5.2 Inventory Operations
- Refill fuel: add liters.
- Simulate sale: subtract liters with insufficient-stock check.
- Update fuel price.
- Enable fuel type for a station (with initial liters + price).
- Disable fuel type (allowed only when stock is `0`).
- All inventory/price changes logged in transactions for audit trail.

## 5.3 Geospatial and Recommendation
- Nearby station search by user location + radius.
- SQL bounding-box filtering + distance ranking.
- Recommendation ranking based on availability, price, and recency.

## 5.4 Map Monitoring
- Station clustering with dynamic marker sizing/color.
- Fullscreen map mode.
- User location + search radius overlay in nearby workflow.
- Area/fuel filters for map views.

---

## 6. SQL Analytics (Implemented)

### Foundational
- Stations by area
- Top stations by highest availability
- Top stations by lowest price
- Joined station + inventory view
- Average price by area
- Total demand per station

### Time-series and demand
- Daily demand trend for selected window
- Hourly demand pattern for selected window
- Demand growth vs previous window (station-level)

### Pricing and competition
- Price spread by area (`min`, `max`, `avg`, `spread`)
- Area fuel mix (liters sold by fuel type per area)

### Revenue and efficiency
- Estimated revenue per station for selected window
- Inventory turnover ratio (`sold / current_stock`)

### Risk
- Dynamic stockout risk report based on selected lookback days
- Estimated days-to-stockout
- Role-filtered risk reporting

---

## 7. Data Engineering Extensions

### 7.1 Raw Transaction Ingestion
Expected CSV/JSON fields:
- `station_id`
- `fuel_type`
- `liters_sold`
- `txn_time`

Run:
```bash
python ingestion.py raw_transactions.csv
```

The job validates required columns, handles nulls/type errors, removes duplicate transaction rows, rejects unknown station IDs, writes valid rows to `transactions`, and records lineage in `pipeline_runs`.

### 7.2 Batch Aggregations
```bash
python transformations.py --days 30
```

Refreshes:
- `daily_demand_summary`
- `inventory_snapshots`
- `revenue_rollups`

### 7.3 Data Quality Checks
```bash
python quality_checks.py --export quality_report.csv
```

Flags:
- outlier fuel prices
- negative inventory
- missing station IDs
- negative sales/restock rows for review

### 7.4 Pipeline Orchestration
One complete run:
```bash
python pipeline_runner.py --source-path raw_transactions.csv --days 30
```

Simulated scheduled refresh every 15 minutes:
```bash
python pipeline_runner.py --days 30 --every-minutes 15
```

### 7.5 S3 Report Export
Local CSV export:
```bash
python s3_export.py daily_demand_summary
```

Upload to S3:
```bash
python s3_export.py daily_demand_summary --bucket your-bucket-name --key-prefix fuel-reports
```

Requires normal AWS credentials through environment variables, shared credentials, or an IAM role.

### 7.6 PostgreSQL Migration
Create the PostgreSQL schema:
```bash
psql "$DATABASE_URL" -f migrate_to_postgres.sql
```

---

## 8. UI Workspaces by Role

## 7.1 Fuel User Workspace
- Nearby Stations:
  - latitude/longitude + radius controls
  - saved location quick reset
  - station list + map
  - best price/nearest KPIs
- Area Insights:
  - area + fuel filters
  - ranking mode switch
  - top-N recommendations
  - average price + spread KPIs
- City Map:
  - map filters by fuel and area

## 7.2 Station Owner Workspace
- Owner Operations:
  - limited to assigned stations only
  - inventory updates (refill/sale/price)
  - fuel type enable/disable
- Owner Analytics:
  - dynamic day window
  - fuel filter
  - top-N controls
  - KPIs: total sales, low stock risk, avg price, estimated revenue, stations managed
  - demand trend, hourly pattern, growth, turnover, risk
- Owner Map:
  - strictly owner-scoped station map

## 7.3 Admin Workspace
- Admin Dashboard:
  - city-level KPI cards for revenue, active stations, stockout alerts, inventory turnover
  - Plotly demand chart by station and date range
  - latest ETL run log table
- Inventory Management:
  - global station/fuel operations
- Advanced Analytics:
  - dynamic time window, area filter, top-N
  - demand, pricing, growth, turnover, risk, revenue, fuel mix
- System Map:
  - city-wide operations map
- Data Quality:
  - anomaly counts
  - rejected records
  - pipeline health status
- User Directory:
  - all users and roles
- Master Setup:
  - create new petrol bunks with initial inventory/prices
  - create new admin users

---

## 9. Data Seeding and Real Data Options

## 8.1 Standard seed
```bash
python data_generator.py --reset --txn-rows 50000
```

Creates:
- stations (realistic brand-style names)
- inventory rows
- transaction history
- users (`admin`, owners, fuel users)
- owner-station assignments

## 8.2 Real station coordinates (OSM)
```bash
python data_generator.py --reset --real --max-real-stations 300 --txn-rows 100000
```

Behavior:
- fetches real fuel station coordinates from OSM for Hyderabad
- maps each station to nearest configured area center
- falls back to synthetic if OSM request fails

---

## 10. Setup and Run

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Seed database:
```bash
python data_generator.py --reset --txn-rows 50000
```

3. Start app:
```bash
python -m streamlit run app.py
```

4. Optional admin signup code override:
```powershell
$env:ADMIN_SIGNUP_CODE="YOUR_SECRET_CODE"
python -m streamlit run app.py
```

---

## 11. Demo Credentials

- Admin: `admin` / `admin123`
- Owners: `owner1`..`owner5` with password `ownerX123`
- Fuel users: `user1`..`user10` with password `userX123`

---

## 12. Important Implementation Notes

- This is a simulation platform, not a payment/billing system.
- Revenue is an **estimated analytical metric** (liters sold × current price).
- Passwords are stored in plain text for demo speed; production should use hashed passwords.
- SQLite remains the local app database; `migrate_to_postgres.sql` provides the PostgreSQL warehouse schema.

---

## 13. Troubleshooting

- `ModuleNotFoundError`:
  - run `pip install -r requirements.txt`

- UI CSS changes not visible:
  - hard refresh browser (`Ctrl + F5`)

- Old station names still visible:
  - run `python data_generator.py --reset`

- Real data fetch fails:
  - retry later (Overpass throttling possible), or use synthetic seed mode

---

## 14. Suggested Next Enhancements

1. Password hashing + secure session handling.
2. Row-level audit table for all admin actions.
3. Demand forecasting model per station/fuel.
4. Alert engine (email/WhatsApp/webhook) for stockout risk.
5. Multi-city tenancy and org-level permissions.
