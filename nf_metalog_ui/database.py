import sqlite3
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class RunInfo:
    """Information about a run."""
    run_name: str
    ingested: datetime
    total_tasks: int


@dataclass
class IdSummary:
    """Aggregated summary for an ID."""
    group_id: str
    total_tasks: int
    submitted: int
    completed: int
    failed: int
    running: int
    cached: int


@dataclass
class ProcessDetail:
    """Detailed information about a process."""
    process: str
    task_id: str
    status: str
    ingested: datetime
    metadata: dict


class MetalogDB:
    """Database connection handler for metalog data."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None
        self.connect()

    def connect(self):
        """Establish database connection."""
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None

    def get_runs(self) -> list[RunInfo]:
        """Get all runs sorted by most recent ingested timestamp."""
        query = """
            SELECT
                run_name,
                MAX(ingested) as last_ingested,
                COUNT(*) as total_tasks
            FROM metalog
            GROUP BY run_name
            ORDER BY last_ingested DESC
        """
        cursor = self.conn.execute(query)
        result = cursor.fetchall()
        return [RunInfo(run_name=r[0], ingested=datetime.fromisoformat(r[1]), total_tasks=r[2]) for r in result]

    def get_id_summary(self, run_name: str) -> list[IdSummary]:
        """Get aggregated summary by ID for a specific run."""
        query = """
            SELECT
                group_id,
                COUNT(*) as total_tasks,
                SUM(CASE WHEN status = 'SUBMITTED' THEN 1 ELSE 0 END) as submitted,
                SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END) as completed,
                SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed,
                SUM(CASE WHEN status = 'RUNNING' THEN 1 ELSE 0 END) as running,
                SUM(CASE WHEN status = 'CACHED' THEN 1 ELSE 0 END) as cached
            FROM metalog
            WHERE run_name = ?
            GROUP BY group_id
            ORDER BY group_id
        """
        cursor = self.conn.execute(query, (run_name,))
        result = cursor.fetchall()
        return [
            IdSummary(
                group_id=row[0],
                total_tasks=row[1],
                submitted=row[2],
                completed=row[3],
                failed=row[4],
                running=row[5],
                cached=row[6]
            ) for row in result
        ]

    def get_process_details(self, run_name: str, group_id: str) -> list[ProcessDetail]:
        """Get detailed process information for a specific run and ID."""
        query = """
            SELECT
                process,
                task_id,
                status,
                ingested,
                metadata
            FROM metalog
            WHERE run_name = ? AND group_id = ?
            ORDER BY ingested DESC
        """
        cursor = self.conn.execute(query, (run_name, group_id))
        result = cursor.fetchall()
        return [
            ProcessDetail(
                process=r[0],
                task_id=r[1],
                status=r[2],
                ingested=datetime.fromisoformat(r[3]),
                metadata=json.loads(r[4]) if r[4] else {}
            ) for r in result
        ]
