# AS400 DB2 Data Mockup Tool

This tool allows you to verify table schemas on an AS400 DB2 system, check journaling status, and generate mockup data for testing and replication scenarios.

## Features
- **Schema Verification**: Validate local JSON schema definitions against actual AS400 DB2 table structures.
- **Journaling Insights**: Report journaling status and retrieve sequence numbers (oldest/newest) from table journals.
- **Bulk Data Generation**: Efficiently generate large volumes of mockup data.
- **Flexible Transactions**: Support configurable ratios of Insert, Update, and Delete operations.
- **Smart Data Mocking**: Powered by the Faker library, supporting realistic names, emails, addresses, and more.
- **Pattern Support**: Custom patterns for increments, random selections, and numeric ranges.
- **Database Optimized**: Uses bulk operations and commits to minimize impact on the AS400 system.
- **Docker Ready**: Fully containerized for easy deployment and testing.

## Prerequisites
- **Python 3.8+**
- **Java Runtime Environment (JRE)** - Required for the JDBC driver.
- **JT400 JDBC Driver**: `jt400.jar` (IBM Toolbox for Java).
  - *Recommended version: 10.0 or higher (Tested with 21.0.6).*

## Installation

### Local Setup
1. Clone the repository.
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Provide the `jt400.jar` file (e.g., place it in a `lib/` directory).

### Docker Setup
1. Ensure Docker and Docker Compose are installed.
2. The provided `Dockerfile` handles the Python environment and JRE installation.

## Configuration
The tool uses a `config.json` file for settings. A template is provided as `config.json.1`.

1. Copy the template:
   ```bash
   cp config.json.1 config.json
   ```
2. Edit `config.json` with your details:
   - `database.driver_path`: Path to your `jt400.jar`.
   - `database.connection_url`: JDBC URL (e.g., `jdbc:as400://HOSTNAME;naming=sql;errors=full`).
   - `database.username` / `password`: Your AS400 credentials.
   - `database.schema`: The target library/schema.
   - `settings.batch_size`: Number of records per bulk operation (default: 1000).

## Quick Start

1. **Define your schema** in `schema_sample.json`.
2. **Define mockup rules** in `mockup_profile_sample.json`.
3. **Run the tool**:
   ```bash
   python main.py
   ```

### Using Docker
Run the application:
```bash
docker-compose up --build
```

Run tests inside Docker:
```bash
docker-compose run as400-mockup python -m unittest test_modules.py
```
  
## Usage Guide

### Transaction Ratios
In your mockup profile, you can define the `transaction_ratio` as `Insert:Update:Delete`.
Example: `"transaction_ratio": "60:30:10"`
- 60% of `total_records` will be Inserts.
- 30% will be Updates (randomly selecting existing PKs).
- 10% will be Deletes (randomly selecting existing PKs).

### Mockup Patterns
The `field_mapping` section supports several types of values:

| Pattern | Description |
| --- | --- |
| `increment` | Auto-incrementing integer (starts at 1 per session). |
| `name`, `email`, `address`, etc. | Any standard [Faker provider](https://faker.readthedocs.io/en/master/providers.html). |
| `random_element(['A', 'B'])` | Pick a random item from a list. |
| `random_int(min=X, max=Y)` | Random integer between X and Y inclusive. |
| `random_number(digits=N)` | Random number with exactly N digits. |
| `date_this_month`, `date_time_this_year` | Formatted date/time strings compatible with DB2. |

## Interactive Features
- Missing Tables: If a table in your schema file doesn't exist, the tool will ask if you want to create it.
- Journaling: If a table isn't journaled, the tool will look for library defaults or ask you where to start journaling.
- Mismatch: If the database structure differs from your schema file, the tool shows the differences and asks whether to Skip, Recreate, or Continue.
  
### Field Name Guessing
If a pattern is not recognized but the field name contains keywords like "name", "email", "postal", "date", or "id", the tool will attempt to generate appropriate mockup data automatically.

## File Descriptions
- `main.py`: Application entry point and orchestration.
- `db_interface.py`: JDBC wrapper for AS400 DB2 operations.
- `mock_generator.py`: Logic for parsing patterns and generating data.
- `schema_sample.json`: Expected table definitions.
- `mockup_profile_sample.json`: Rules for data generation and transaction mix.
