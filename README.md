# nf-metalog-ui

TUI (Terminal User Interface) for visualizing nf-metalog DuckDB data using Textual.

## Features

- **Run Selector**: Filter by run_name, auto-selects most recent run by default
- **ID Summary Table**: Aggregated view showing task counts by status (completed/failed/running)
- **Process Detail Table**: Drill down into individual processes for selected ID
- **Metadata Pane**: View full JSON metadata for selected process
- **Auto-refresh**: Data refreshes every 5 seconds automatically
- **Color-coded Status**: Green (completed), Red (failed), Yellow (running)
- **Keyboard Navigation**: Arrow keys to navigate, Tab to switch between panes, Q to quit, R to refresh

## Installation

```bash
uv sync
```

## Usage

```bash
uv run metalog-ui <path-to-duckdb-file>
```

Example:
```bash
uv run metalog-ui /path/to/nf-metalog/metalog.db
```

## Testing

Create a test database with sample data:

```bash
uv run python create_test_db.py
uv run metalog-ui test_metalog.db
```

## Navigation

- **Arrow Keys**: Navigate within tables
- **Tab**: Switch between widgets
- **Q**: Quit application
- **R**: Manual refresh
- Data auto-refreshes every 5 seconds

## Database Schema

The tool expects a DuckDB database with the following schema:

```sql
CREATE TABLE metalog (
    run_name VARCHAR,
    id VARCHAR,
    ingested TIMESTAMPTZ,
    process VARCHAR,
    task_id VARCHAR,
    status VARCHAR,
    metadata JSON
)
```
