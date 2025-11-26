import argparse
import sys
from datetime import datetime
from pathlib import Path

from textual.app import App, ComposeResult
from textual.containers import Container, Horizontal, Vertical
from textual.widgets import DataTable, Footer, Header, Label, Select, Static
from textual.widgets import Pretty
from textual import on

from nf_metalog_ui.database import MetalogDB


class RunSelector(Container):
    """Widget for selecting a run."""

    # DEFAULT_CSS = """
    # RunSelector {
    #     height: 3;
    #     background: $panel;
    #     padding: 1;
    # }
    # """

    def __init__(self, db: MetalogDB):
        super().__init__()
        self.db = db
        self.selected_run = None

    def compose(self) -> ComposeResult:
        yield Label("Pipeline run: ", id="run-label")
        yield Select([("Loading...", "")], id="run-select", allow_blank=True)

    def on_mount(self):
        """Load runs when mounted."""
        self.refresh_runs()

    def refresh_runs(self):
        """Refresh the list of runs."""
        runs = self.db.get_runs()
        select = self.query_one("#run-select", Select)

        if runs:
            options = [(f"{r.run_name} (Last: {r.ingested.strftime('%Y-%m-%d %H:%M:%S')}, Tasks: {r.total_tasks})", r.run_name) for r in runs]
            select.set_options(options)
            if self.selected_run is None:
                # Auto-select the most recent run
                select.value = runs[0].run_name
                self.selected_run = runs[0].run_name
        else:
            # Print a message
            pass

    @on(Select.Changed, "#run-select")
    def on_run_changed(self, event: Select.Changed):
        """Handle run selection change."""
        if event.value != Select.BLANK:
            self.selected_run = event.value
            self.app.selected_run = event.value


class IdSummaryTable(Container):
    """Widget displaying aggregated ID summary."""

    # DEFAULT_CSS = """
    # IdSummaryTable {
    #     height: 50%;
    #     border: solid $primary;
    # }
    # """

    def __init__(self, db: MetalogDB):
        super().__init__()
        self.db = db
        self.selected_id = None

    def compose(self) -> ComposeResult:
        yield Label("Run tasks summary", id="id-summary-title")
        table = DataTable(id="id-table", cursor_type="row")
        table.add_columns("Task id", "Total", "Submitted", "Running", "Completed", "Failed", "Cached")
        yield table

    def refresh_data(self, run_name: str):
        """Refresh the ID summary data."""
        table = self.query_one("#id-table", DataTable)
        table.clear()

        summaries = self.db.get_id_summary(run_name)
        for summary in summaries:
            table.add_row(
                summary.group_id,
                str(summary.total_tasks),
                f"[cyan]{summary.submitted or "0"}[/cyan]",
                f"[yellow]{summary.running or "0"}[/yellow]",
                f"[green]{summary.completed or "0"}[/green]",
                f"[red]{summary.failed or "0"}[/red]",
                f"[blue]{summary.cached or "0"}[/blue]"
            )

        # Auto-select first row if available and no selection exists
        if len(summaries) and self.selected_id is None:
            table.move_cursor(row=0)
            self.selected_id = summaries[0].group_id

    @on(DataTable.RowHighlighted, "#id-table")
    def on_id_selected(self, event: DataTable.RowHighlighted):
        """Handle ID row selection."""
        if event.cursor_row >= 0:
            table = self.query_one("#id-table", DataTable)
            row_key = event.row_key
            row = table.get_row(row_key)
            self.selected_id = row[0]
            self.app.selected_id = row[0]


class ProcessDetailTable(Container):
    """Widget displaying process details."""

    # DEFAULT_CSS = """
    # ProcessDetailTable {
    #     height: 50%;
    #     border: solid $primary;
    # }
    # """

    def __init__(self, db: MetalogDB):
        super().__init__()
        self.db = db
        self.selected_process = None

    def compose(self) -> ComposeResult:
        yield Label("Process Details", id="process-detail-title")
        table = DataTable(id="process-table", cursor_type="row")
        table.add_columns("Process", "Task ID", "Status", "Ingested")
        yield table

    def refresh_data(self, run_name: str, group_id: str):
        """Refresh the process detail data."""
        table = self.query_one("#process-table", DataTable)
        table.clear()

        details = self.db.get_process_details(run_name, group_id)
        for detail in details:
            # Map status to color
            status_colors = {
                "COMPLETED": "green",
                "FAILED": "red",
                "RUNNING": "yellow",
                "SUBMITTED": "cyan",
                "CACHED": "blue"
            }
            status_color = status_colors.get(detail.status, "white")
            table.add_row(
                detail.process,
                detail.task_id,
                f"[{status_color}]{detail.status}[/{status_color}]",
                detail.ingested.strftime('%Y-%m-%d %H:%M:%S')
            )

    @on(DataTable.RowHighlighted, "#process-table")
    def on_process_selected(self, event: DataTable.RowHighlighted):
        """Handle process row selection."""
        if event.cursor_row >= 0:
            table = self.query_one("#process-table", DataTable)
            row_key = event.row_key
            row = table.get_row(row_key)
            self.selected_process = (row[0], row[1])  # (process, task_id)
            self.app.selected_process = self.selected_process


class MetadataPane(Container):
    """Widget displaying metadata details."""

    DEFAULT_CSS = """
    MetadataPane {
        width: 40%;
        border: solid $primary;
        padding: 1;
    }
    """

    def __init__(self, db: MetalogDB):
        super().__init__()
        self.db = db

    def compose(self) -> ComposeResult:
        yield Label("Process information", id="metadata-title")
        yield Pretty("No selection", id="metadata-content")

    def refresh_data(self, run_name: str, group_id: str, process: str, task_id: str):
        """Refresh metadata for selected process."""
        details = self.db.get_process_details(run_name, group_id)

        # Find the matching process
        for detail in details:
            if detail.process == process and detail.task_id == task_id:
                content = self.query_one("#metadata-content", Pretty)
                if detail.metadata:
                    content.update(detail.metadata)
                else:
                    content.update("No process data available")
                break


class MetalogApp(App):
    """Main TUI application for nf-metalog visualization."""

    # CSS = """
    # Screen {
    #     background: $surface;
    # }
    #
    # #main-container {
    #     height: 100%;
    # }
    #
    # #left-panel {
    #     width: 60%;
    # }
    #
    # #status-bar {
    #     height: 1;
    #     background: $panel;
    #     color: $text-muted;
    #     padding: 0 1;
    # }
    # """

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("r", "refresh", "Refresh"),
    ]

    def __init__(self, db_path: str):
        super().__init__()
        self.db = MetalogDB(db_path)
        self.selected_run = None
        self.selected_id = None
        self.selected_process = None
        self.last_refresh = None

    def compose(self) -> ComposeResult:
        yield Header()
        yield RunSelector(self.db)
        with Horizontal(id="main-container"):
            with Vertical(id="left-panel"):
                yield IdSummaryTable(self.db)
                yield ProcessDetailTable(self.db)
            yield MetadataPane(self.db)
        yield Label("", id="status-bar")
        yield Footer()

    def on_mount(self):
        """Initialize database connection and start auto-refresh."""
        self.db.connect()
        self.set_interval(5.0, self.auto_refresh)
        self.update_status("Connected to database")

    def watch_selected_run(self, new_run: str):
        """Watch for run selection changes."""
        if new_run:
            id_table = self.query_one(IdSummaryTable)
            id_table.refresh_data(new_run)
            self.update_status(f"Viewing pipeline run: {new_run}")

    def watch_selected_id(self, new_id: str):
        """Watch for ID selection changes."""
        if new_id and self.selected_run:
            process_table = self.query_one(ProcessDetailTable)
            process_table.refresh_data(self.selected_run, new_id)

    def watch_selected_process(self, new_process: tuple):
        """Watch for process selection changes."""
        if new_process and self.selected_run and self.selected_id:
            metadata_pane = self.query_one(MetadataPane)
            metadata_pane.refresh_data(self.selected_run, self.selected_id, new_process[0], new_process[1])

    def auto_refresh(self):
        """Auto-refresh data every 5 seconds."""
        run_selector = self.query_one(RunSelector)
        run_selector.refresh_runs()

        if self.selected_run:
            id_table = self.query_one(IdSummaryTable)
            id_table.refresh_data(self.selected_run)

        if self.selected_run and self.selected_id:
            process_table = self.query_one(ProcessDetailTable)
            process_table.refresh_data(self.selected_run, self.selected_id)

        self.last_refresh = datetime.now()
        self.update_status(f"Last refresh: {self.last_refresh.strftime('%H:%M:%S')}")

    def action_refresh(self):
        """Manual refresh action."""
        self.auto_refresh()

    def update_status(self, message: str):
        """Update status bar message."""
        status = self.query_one("#status-bar", Label)
        status.update(message)

    def on_unmount(self):
        """Clean up database connection."""
        self.db.close()


def main():
    """Entry point for the application."""
    parser = argparse.ArgumentParser(description="TUI for visualizing nf-metalog SQLite data")
    parser.add_argument("db_path", help="Path to the SQLite database file")
    args = parser.parse_args()

    db_path = Path(args.db_path)
    if not db_path.exists():
        print(f"Error: Database file not found: {db_path}", file=sys.stderr)
        sys.exit(1)

    app = MetalogApp(str(db_path))
    app.run()


if __name__ == "__main__":
    main()
