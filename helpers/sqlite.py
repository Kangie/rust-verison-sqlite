import semver
import sqlite3
from typing import (
    List,
    Tuple,
    Any,
    Optional,
    Iterable,
)  # Added Iterable for executemany

# Import RustVersion for type hinting, assuming it's accessible
# If not, consider using generic types or forward references if needed.
# Option 1: Direct import (if structure allows)
# from rustup_manifest_ingestor import log, RustVersion
# Option 2: Placeholder/Generic Typing (if import causes issues)
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from rustup_manifest_ingestor import (
        RustVersion,
        log,
    )  # Use forward reference for type checking only
else:
    # Runtime fallback if direct import isn't feasible or causes circular deps
    # This means type hints for RustVersion might not be fully checked by static analyzers at runtime install
    # but code relying on it should still work if the structure is correct.
    # A better solution might be to move RustVersion to a shared 'models.py'
    RustVersion = Any
    import logging

    log = logging.getLogger(__name__)  # Basic logger if structlog isn't setup here

DB_SCHEMA = [
    {
        "name": "rust_versions",
        "columns": [
            {"name": "version", "type": "TEXT", "primary_key": True, "not_null": True},
            {
                "name": "release_date",
                "type": "TEXT",
                "not_null": True,
            },  # Storing as text YYYY-MM-DD
            {
                "name": "latest_stable",
                "type": "INTEGER",
                "default": 0,
                "not_null": True,
            },
            {"name": "latest_beta", "type": "INTEGER", "default": 0, "not_null": True},
            {
                "name": "latest_nightly",
                "type": "INTEGER",
                "default": 0,
                "not_null": True,
            },
        ],
        # No table-level constraints for this one
    },
    {
        "name": "components",
        "columns": [
            {
                "name": "id",
                "type": "INTEGER",
                "primary_key": True,
            },  # Autoincrement is default for INTEGER PK
            {"name": "name", "type": "TEXT", "not_null": True},
            {"name": "version", "type": "TEXT", "not_null": True},
            {"name": "rust_version", "type": "TEXT", "not_null": True},
            {"name": "git_commit", "type": "TEXT"},  # Allowed to be NULL
            {
                "name": "profile_complete",
                "type": "INTEGER",
                "default": 0,
                "not_null": True,
            },
            {
                "name": "profile_default",
                "type": "INTEGER",
                "default": 0,
                "not_null": True,
            },
            {
                "name": "profile_minimal",
                "type": "INTEGER",
                "default": 0,
                "not_null": True,
            },
        ],
        "foreign_keys": [
            {
                "columns": ["rust_version"],
                "references_table": "rust_versions",
                "references_columns": ["version"],
                "on_delete": "CASCADE",
            }
        ],
        "unique_constraints": [
            # Ensure a component name (like 'rustc') is unique for a specific rust_version
            ["rust_version", "name"]
        ],
    },
    {
        "name": "targets",
        "columns": [
            {"name": "id", "type": "INTEGER", "primary_key": True},
            {"name": "name", "type": "TEXT", "not_null": True},
            {"name": "url", "type": "TEXT", "not_null": True},
            {"name": "hash", "type": "TEXT", "not_null": True},
            {"name": "component_id", "type": "INTEGER", "not_null": True},
        ],
        "foreign_keys": [
            {
                "columns": ["component_id"],
                "references_table": "components",
                "references_columns": ["id"],
                "on_delete": "CASCADE",
            }
        ],
        # Can add table-level unique constraints here too if needed, e.g., UNIQUE(name, component_id)
    },
    {
        "name": "artefacts",
        "columns": [
            {"name": "id", "type": "INTEGER", "primary_key": True},
            {"name": "rust_version", "type": "TEXT", "not_null": True},
            {"name": "type", "type": "INTEGER", "not_null": True},  # Or TEXT
            {"name": "target", "type": "TEXT", "not_null": True},
            {"name": "url", "type": "TEXT", "not_null": True, "unique": True},
            {"name": "hash", "type": "TEXT", "not_null": True},
        ],
        "foreign_keys": [
            {
                "columns": ["rust_version"],
                "references_table": "rust_versions",
                "references_columns": ["version"],
                "on_delete": "CASCADE",
            }
        ],
    },
]


# --- Parameterized Query Execution ---


def execute_write_query(
    db_connection: sqlite3.Connection, query: str, values: Optional[Tuple] = None
) -> int:
    """
    Executes a single parameterized write (INSERT, UPDATE, DELETE) query
    using the provided database connection.

    Does NOT commit the transaction. Transaction management should be handled externally.

    Args:
        db_connection: The active SQLite database connection.
        query: The SQL query string with placeholders (e.g., ?, ?).
        values: A tuple of values corresponding to the placeholders in the query.

    Returns:
        The number of rows affected by the query.

    Raises:
        sqlite3.Error: If any database error occurs during execution.
    """
    try:
        cursor = db_connection.cursor()
        log.debug("Executing write query", query=query, params=values)
        if values:
            cursor.execute(query, values)
        else:
            cursor.execute(query)
        rowcount = cursor.rowcount
        log.debug(f"Query affected {rowcount} rows")
        return rowcount
    except sqlite3.Error as error:
        log.error(
            "Failed to execute write query",
            query=query,
            params=values,
            error=str(error),
        )
        # Re-raise the original error for external handling (e.g., rollback)
        raise error


def execute_many_write_query(
    db_connection: sqlite3.Connection, query: str, values_list: Iterable[Tuple]
) -> int:
    """
    Executes a parameterized write query (INSERT, UPDATE, DELETE) for multiple sets
    of values using the provided database connection (executemany).

    Does NOT commit the transaction. Transaction management should be handled externally.

    Args:
        db_connection: The active SQLite database connection.
        query: The SQL query string with placeholders (e.g., ?, ?).
        values_list: An iterable (e.g., list) of tuples, where each tuple contains
                     values for one execution of the query.

    Returns:
        The total number of rows affected.

    Raises:
        sqlite3.Error: If any database error occurs during execution.
    """
    try:
        cursor = db_connection.cursor()
        log.debug(
            "Executing batch write query (first few params shown)",
            query=query,
            params_count=len(list(values_list)),
            first_params=list(values_list)[:3],
        )
        cursor.executemany(query, values_list)
        rowcount = cursor.rowcount
        log.debug(f"Batch query affected {rowcount} rows")
        return rowcount
    except sqlite3.Error as error:
        log.error(
            "Query and values_list for debugging",
            query=query,
            values_list=list(values_list)[:5],  # Log first 5 for brevity
        )
        # Write query and values_list to a file for debugging
        with open("./sqlite_debug.log", "a") as debug_file:
            debug_file.write(f"Query: {query}\n")
            debug_file.write(f"Values List (first 5): {list(values_list)}\n\n")
        log.error("Failed to execute batch write query", query=query, error=str(error))
        raise error


def fetch_one(
    db_connection: sqlite3.Connection, query: str, values: Optional[Tuple] = None
) -> Optional[Tuple]:
    """
    Executes a SELECT query expected to return at most one row.

    Args:
        db_connection: The active SQLite database connection.
        query: The SQL query string with optional placeholders.
        values: Optional tuple of values for placeholders.

    Returns:
        A single tuple representing the row, or None if no row is found.

    Raises:
        sqlite3.Error: If any database error occurs.
    """
    try:
        cursor = db_connection.cursor()
        log.debug("Executing fetch_one query", query=query, params=values)
        if values:
            cursor.execute(query, values)
        else:
            cursor.execute(query)
        result = cursor.fetchone()
        return result
    except sqlite3.Error as error:
        log.error(
            "Failed to execute fetch_one query",
            query=query,
            params=values,
            error=str(error),
        )
        raise error


def fetch_all(
    db_connection: sqlite3.Connection, query: str, values: Optional[Tuple] = None
) -> List[Tuple]:
    """
    Executes a SELECT query and returns all resulting rows.

    Args:
        db_connection: The active SQLite database connection.
        query: The SQL query string with optional placeholders.
        values: Optional tuple of values for placeholders.

    Returns:
        A list of tuples, where each tuple represents a row. Empty list if no rows.

    Raises:
        sqlite3.Error: If any database error occurs.
    """
    try:
        cursor = db_connection.cursor()
        log.debug("Executing fetch_all query", query=query, params=values)
        if values:
            cursor.execute(query, values)
        else:
            cursor.execute(query)
        results = cursor.fetchall()
        return results
    except sqlite3.Error as error:
        log.error(
            "Failed to execute fetch_all query",
            query=query,
            params=values,
            error=str(error),
        )
        raise error


# --- Schema Initialization ---


def init_tables(db_connection: sqlite3.Connection):
    """
    Initialises tables in the database using the DB_SCHEMA structure.

    Generates and executes CREATE TABLE IF NOT EXISTS statements dynamically.

    Args:
        db_connection: The active SQLite database connection.
    """
    log.info("Initializing database schema from structure...")
    cursor = db_connection.cursor()

    for table in DB_SCHEMA:
        table_name = table["name"]
        definitions = []

        # 1. Process Columns
        for col in table["columns"]:
            col_def = [f"`{col['name']}`", col["type"]]
            if col.get("primary_key"):
                col_def.append("PRIMARY KEY")
                # INTEGER PRIMARY KEY automatically gets AUTOINCREMENT in SQLite
            if col.get("not_null"):
                # Primary keys are implicitly NOT NULL
                if not col.get("primary_key"):
                    col_def.append("NOT NULL")
            if col.get("unique"):
                col_def.append("UNIQUE")
            if "default" in col:
                default_val = col["default"]
                # Quote string defaults, but not numbers or NULL
                if isinstance(default_val, str):
                    col_def.append(f"DEFAULT '{default_val}'")
                elif default_val is None:
                    col_def.append("DEFAULT NULL")
                else:  # Assumed numeric or boolean (stored as 0/1)
                    col_def.append(f"DEFAULT {default_val}")

            definitions.append(" ".join(col_def))

        # 2. Process Table-level Unique Constraints
        if "unique_constraints" in table:
            for constraint_cols in table["unique_constraints"]:
                cols_str = ", ".join([f"`{c}`" for c in constraint_cols])
                definitions.append(f"UNIQUE ({cols_str})")

        # 3. Process Foreign Keys
        if "foreign_keys" in table:
            for fk in table["foreign_keys"]:
                local_cols = ", ".join([f"`{c}`" for c in fk["columns"]])
                ref_table = fk["references_table"]
                ref_cols = ", ".join([f"`{c}`" for c in fk["references_columns"]])
                fk_def = (
                    f"FOREIGN KEY ({local_cols}) REFERENCES `{ref_table}` ({ref_cols})"
                )
                if fk.get("on_delete"):
                    fk_def += f" ON DELETE {fk['on_delete'].upper()}"  # e.g., ON DELETE CASCADE
                # Add ON UPDATE if needed similarly
                definitions.append(fk_def)

        # 4. Assemble CREATE TABLE statement
        # Use IF NOT EXISTS for idempotency
        statement = f"CREATE TABLE IF NOT EXISTS `{table_name}` (\n    "
        statement += ",\n    ".join(definitions)
        statement += "\n);"

        log.debug(f"Executing schema statement for table: {table_name}")
        # For full debugging: log.debug(statement)
        try:
            cursor.execute(statement)
        except sqlite3.Error as e:
            log.error(
                f"Failed to execute CREATE TABLE statement for {table_name}",
                error=str(e),
                statement=statement,
            )
            raise e  # Propagate error

    log.info("Schema initialization complete.")


# --- Data Access Functions ---


def get_id_for_component(
    db_connection: sqlite3.Connection, component_name: str, rust_version: str
) -> Optional[int]:
    """
    Retrieve the ID of a component using a parameterized query.

    Args:
        db_connection: The active SQLite database connection.
        component_name: The name of the component.
        rust_version: The associated Rust version.

    Returns:
        The component ID (int) if found, otherwise None.
    """
    query = "SELECT id FROM components WHERE name = ? AND rust_version = ?;"
    result = fetch_one(db_connection, query, (component_name, rust_version))
    return result[0] if result else None


def insert_rust_version(db_connection: sqlite3.Connection, rust: "RustVersion"):
    """
    Inserts a Rust version and its associated components, targets, and artefacts
    into the database using parameterized queries and batch operations.

    Assumes the version does not already exist. Use INSERT OR IGNORE or handle
    IntegrityError externally if needed. Transaction management is external.

    Args:
        db_connection: The active SQLite database connection.
        rust: The RustVersion object to insert.

    Raises:
        sqlite3.Error: If any database error occurs.
        ValueError: If component ID lookup fails unexpectedly.
    """
    log.info(f"Preparing to insert Rust version {rust.version} and related data.")

    # 1. Insert the main Rust version entry
    version_query = """
        INSERT INTO rust_versions (version, release_date, latest_stable, latest_beta, latest_nightly)
        VALUES (?, ?, ?, ?, ?);
    """
    # Flags are typically set later by set_rust_channel_flags, inserting default 0
    version_values = (
        rust.version,
        rust.release_date,
        1 if rust.latest_stable else 0,  # Use flags from object if available, else 0
        1 if rust.latest_beta else 0,
        1 if rust.latest_nightly else 0,
    )
    execute_write_query(db_connection, version_query, version_values)
    log.info(f"Inserted base entry for version {rust.version}")

    # 2. Insert Components using executemany
    component_query = """
        INSERT INTO components (name, version, rust_version, git_commit, profile_complete, profile_default, profile_minimal)
        VALUES (?, ?, ?, ?, ?, ?, ?);
    """
    component_values = [
        (
            comp.name,
            comp.version,
            comp.rust_version,  # Should match rust.version
            comp.git_commit,
            0,  # Profiles set later or based on data if available here
            0,
            0,
        )
        for comp in rust.components
    ]
    if component_values:
        execute_many_write_query(db_connection, component_query, component_values)
        log.info(
            f"Inserted {len(component_values)} components for version {rust.version}"
        )

        # 3. Insert Targets using executemany (requires component IDs)
        target_query = """
            INSERT INTO targets (name, url, hash, component_id)
            VALUES (?, ?, ?, ?);
        """
        target_values = []
        for comp in rust.components:
            # Get the ID of the component we just inserted (or assume it exists if using INSERT OR IGNORE)
            component_id = get_id_for_component(db_connection, comp.name, rust.version)
            if component_id is None:
                # This shouldn't happen if component insert succeeded unless there's a race condition
                # or transaction isolation issue (unlikely with single connection script).
                # If components might already exist, fetching ID before insert might be needed.
                log.error(
                    "Failed to retrieve ID for recently inserted component",
                    component_name=comp.name,
                    rust_version=rust.version,
                )
                # Raise an error, as target insertion will fail.
                raise ValueError(
                    f"Could not find component ID for {comp.name} ({rust.version}) after insertion."
                )

            for target in comp.targets:
                if target.url and target.hash:  # Ensure essential data is present
                    target_values.append(
                        (target.name, target.url, target.hash, component_id)
                    )

        if target_values:
            execute_many_write_query(db_connection, target_query, target_values)
            log.info(
                f"Inserted {len(target_values)} targets for version {rust.version}"
            )

    # 4. Insert Artefacts using executemany
    artefact_query = """
        INSERT INTO artefacts (rust_version, type, target, url, hash)
        VALUES (?, ?, ?, ?, ?);
    """
    artefact_values = [
        (
            rust.version,
            artefact.type.value,
            artefact.target,
            artefact.url,
            artefact.hash,
        )
        for artefact in rust.artefacts
        if artefact.url and artefact.hash
    ]
    if artefact_values:
        execute_many_write_query(db_connection, artefact_query, artefact_values)
        log.info(
            f"Inserted {len(artefact_values)} artefacts for version {rust.version}"
        )

    # 5. Update Profile Flags (if applicable) - Requires semver, ensure installed
    if rust.profiles:  # Check if profile data exists
        if semver.compare(rust.version.split('-')[0], "1.32.0") >= 0:
            log.info(f"Updating profile flags for Rust {rust.version}")
            for profile, components_in_profile in rust.profiles.items():
                if components_in_profile:  # Ensure list is not empty
                    # Create placeholders for the IN clause
                    placeholders = ",".join("?" * len(components_in_profile))
                    # Use safe profile column name (assuming profiles are 'complete', 'default', 'minimal')
                    profile_col = f"profile_{profile}"  # Be careful with dynamically generating column names
                    if profile_col not in [
                        "profile_complete",
                        "profile_default",
                        "profile_minimal",
                    ]:
                        log.warning(
                            f"Unknown profile type '{profile}', skipping flag update."
                        )
                        continue

                    # Construct query safely using placeholders
                    # Need to use execute_write_query as executemany doesn't work well with dynamic IN clauses
                    profile_update_query = f"""
                        UPDATE components SET {profile_col} = 1
                        WHERE rust_version = ? AND name IN ({placeholders});
                    """
                    profile_update_values = (rust.version,) + tuple(components_in_profile)
                    execute_write_query(
                        db_connection, profile_update_query, profile_update_values
                    )
        else:
            log.debug(f"Skipping profile flags for Rust {rust.version} (older than 1.32.0)")


# --- Read Functions ---


def get_rust_version_strings(db_connection: sqlite3.Connection) -> List[str]:
    """
    Retrieve all Rust version strings from the database.

    Args:
        db_connection: The active SQLite database connection.

    Returns:
        A list of Rust version strings.
    """
    query = "SELECT version FROM rust_versions ORDER BY version DESC;"
    results = fetch_all(db_connection, query)
    return [row[0] for row in results]


def get_existing_versions(
    db_connection: sqlite3.Connection, versions_list: List[str]
) -> set[str]:
    """
    Checks which versions from the provided list already exist in the database.

    Args:
        db_connection: The active SQLite database connection.
        versions_list: A list of version strings to check.

    Returns:
        A set containing the version strings from the input list that exist in the db.
    """
    if not versions_list:
        return set()

    placeholders = ",".join("?" * len(versions_list))
    query = f"SELECT version FROM rust_versions WHERE version IN ({placeholders});"
    results = fetch_all(db_connection, query, tuple(versions_list))
    return {row[0] for row in results}


def get_rust_versions(db_connection: sqlite3.Connection) -> list[dict]:
    """
    Retrieve core Rust version information from the database.
    NOTE: This returns a list of dictionaries, not fully populated RustVersion objects,
          as components/targets/artefacts are not fetched here for performance.

    Args:
        db_connection: The active SQLite database connection.

    Returns:
        A list of dictionaries, each containing 'version', 'release_date', 'latest_stable', etc.
    """
    query = """
        SELECT version, release_date, latest_stable, latest_beta, latest_nightly
        FROM rust_versions ORDER BY release_date DESC;
    """
    results = fetch_all(db_connection, query)
    # Return as list of dicts for easier use if not building full objects
    return [
        {
            "version": row[0],
            "release_date": row[1],
            "latest_stable": bool(row[2]),
            "latest_beta": bool(row[3]),
            "latest_nightly": bool(row[4]),
        }
        for row in results
    ]


# --- Update/Delete Functions ---


def set_rust_channel_flags(
    db_connection: sqlite3.Connection,
    stable: Optional[str],
    beta: Optional[str],
    nightly: Optional[str],
):
    """
    Sets the latest_stable, latest_beta, and latest_nightly flags using
    a single parameterized UPDATE query.

    Args:
        db_connection: The active SQLite database connection.
        stable: The version string for the latest stable release, or None.
        beta: The version string for the latest beta release, or None.
        nightly: The version string for the latest nightly release, or None.
    """
    log.info(
        "Setting latest channel flags in the database",
        stable=stable,
        beta=beta,
        nightly=nightly,
    )
    query = """
        UPDATE rust_versions
        SET
            latest_stable = CASE WHEN version = ? THEN 1 ELSE 0 END,
            latest_beta = CASE WHEN version = ? THEN 1 ELSE 0 END,
            latest_nightly = CASE WHEN version = ? THEN 1 ELSE 0 END;
    """
    # Pass values even if None, SQL CASE WHEN handles comparison correctly (version = NULL is false)
    values = (stable, beta, nightly)
    execute_write_query(db_connection, query, values)


def delete_version_data(db_connection: sqlite3.Connection, version: str):
    """
    Deletes a Rust version and all its associated data (components, targets, artefacts)
    using CASCADE delete defined in the schema.

    Args:
        db_connection: The active SQLite database connection.
        version: The Rust version string to delete.
    """
    log.warning(f"Deleting all data for Rust version {version}")
    query = "DELETE FROM rust_versions WHERE version = ?;"
    execute_write_query(db_connection, query, (version,))


def delete_nightly(db_connection: sqlite3.Connection):
    """
    Deletes all rows marked as latest_nightly. Should typically only delete one.
    This relies on the latest_nightly flag being correctly maintained.
    """
    log.info("Deleting existing nightly version(s) based on flag")
    # We can leverage the CASCADE DELETE by just deleting from rust_versions
    query = "DELETE FROM rust_versions WHERE latest_nightly = 1;"
    execute_write_query(db_connection, query)
