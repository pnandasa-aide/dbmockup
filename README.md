# AS400 DB2 Data Mockup Tool

This tool allows you to verify table schemas on an AS400 DB2 system, check journaling status, and generate mockup data for testing and replication scenarios.

## Features
- Schema verification against a JSON definition.
- Journaling status and info reporting.
- Bulk data generation and transactions (Insert, Update, Delete) based on a mockup profile.
- Support for realistic data patterns using the Faker library.
- Bulk commit to minimize database impact.

## Prerequisites
- **Python 3.x**
- **Java Runtime Environment (JRE)** - required for the JT400 JDBC driver.
- **JT400 JAR** - You must provide the `jt400.jar` file (IBM Toolbox for Java).

## Installation
1. Install the required Python packages:
   ```bash
   pip install -r requirements.txt
   ```

## Configuration
Edit `config.json` to provide your database connection details:
- `driver_path`: The local path to your `jt400.jar`.
- `connection_url`: The JDBC connection string for your AS400.
- `username`/`password`: Your database credentials.
- `schema`: The target schema/library.

## Usage
Run the main script:
```bash
python main.py
```

## File Descriptions
- `main.py`: The entry point for the application.
- `db_interface.py`: Handles database connections and operations.
- `mock_generator.py`: Generates realistic mockup data.
- `schema_sample.json`: Defines the expected table structures.
- `mockup_profile_sample.json`: Defines data generation rules and transaction ratios.
- `config.json`: Main configuration file.

## Customizing Mockup Data
The `mockup_profile_sample.json` uses patterns that map to Faker methods or custom logic:
- `increment`: Auto-incrementing integer.
- `name`, `email`, `postcode`: Standard Faker providers.
- `random_element(['A', 'B'])`: Selects a random value from a list.
- `random_int(min=X, max=Y)`: Generates a random integer in range.
- `random_number(digits=N)`: Generates a random number with N digits.
