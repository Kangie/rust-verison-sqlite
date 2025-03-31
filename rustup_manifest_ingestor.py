#!/usr/bin/env python3

# This script is used to ingest rust release information into an sqlite database for later querying.

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from enum import Enum
from tqdm import tqdm  # Use tqdm directly with executor.map for better integration
from typing import List, Dict, Optional, Tuple, Set
import argparse
import helpers.sqlite as sq
import os
import re
import requests
import sqlite3
import structlog
import sys
import tomllib as toml

# --- Constants ---
DIST_SERVER_BASE_URL = "https://static.rust-lang.org"
MANIFESTS_URL = f"{DIST_SERVER_BASE_URL}/manifests.txt"
DEFAULT_DB_PATH = "./rust_versions.sqlite3"
LOGLEVEL = os.environ.get("LOGLEVEL", "INFO").upper()
VERSION = "rust-version-parser 0.2"

# --- Configuration ---
structlog.configure(wrapper_class=structlog.make_filtering_bound_logger(LOGLEVEL))
log = structlog.get_logger()

# --- Data Classes ---


@dataclass
class Target:
    name: str
    url: str
    hash: str


@dataclass
class Component:
    name: str
    version: str
    rust_version: str
    git_commit: Optional[str]
    targets: List[Target]


@dataclass(frozen=True)
class ArtefactType(Enum):
    installer_msi = 1
    installer_pkg = 2
    source_code = 3


@dataclass
class Artefact:
    type: ArtefactType
    url: str
    hash: str
    target: str


@dataclass
class RustVersion:
    version: str
    release_date: str
    manifest_url: str
    components: List[Component]
    profiles: Optional[Dict[str, List[str]]] = None
    renames: Optional[Dict[str, str]] = None
    artefacts: Optional[List[Artefact]] = None
    latest_stable: bool = False
    latest_beta: bool = False
    latest_nightly: bool = False


# --- Core Logic ---


def fetch_manifest_list() -> List[str]:
    """
    Fetches the list of Rust manifest URLs from the official Rust static URL.

    Returns:
        A list of manifest URL paths (relative to DIST_SERVER_BASE_URL).

    Raises:
        requests.exceptions.RequestException: If the HTTP request fails.
        SystemExit: If the request fails and exits the script.
    """
    log.debug(f"Fetching manifest list from {MANIFESTS_URL}")
    try:
        r = requests.get(MANIFESTS_URL, timeout=30)
        r.raise_for_status()
        return [line for line in r.text.splitlines() if line]
    except requests.exceptions.RequestException as e:
        log.error("Failed to fetch manifest list", url=MANIFESTS_URL, error=str(e))
        sys.exit(
            f"Fatal Error: Could not fetch initial manifest list from {MANIFESTS_URL}. Exiting."
        )


def filter_and_sort_manifests(
    manifest_paths: List[str],
) -> Tuple[List[str], Optional[str], Optional[str], Optional[str]]:
    """
    Filters manifest paths to keep only versioned stable/beta releases
    (newest entry for specific historical duplicates) and identifies the
    single latest paths for the stable, beta, and nightly channels.

    Args:
        manifest_paths: A list of manifest URL paths (e.g., 'dist/2020-01-01/file.toml'),
                       assumed to be roughly chronological (oldest first).

    Returns:
        A tuple containing:
            - Filtered list of versioned stable/beta manifest paths (newest first).
            - Path to the latest stable channel manifest.
            - Path to the latest beta channel manifest.
            - Path to the latest nightly channel manifest.
    """
    latest_nightly = None
    latest_beta = None
    latest_stable = None
    processed_manifests = []
    handled_duplicates: Set[str] = set()
    versions_to_deduplicate = {"1.8.0", "1.14.0", "1.15.1", "1.49.0"}

    # --- Identify latest generic channel manifests first ---
    # These paths often act like symlinks pointing to the actual latest content.
    for path in manifest_paths:
        if "channel-rust-nightly.toml" in path:
            latest_nightly = path
        if "channel-rust-beta.toml" in path:
            latest_beta = path
        if "channel-rust-stable.toml" in path:
            latest_stable = path
    log.debug(
        "Identified latest channel paths",
        stable=latest_stable,
        beta=latest_beta,
        nightly=latest_nightly,
    )

    # --- Process paths for filtering, iterating newest-to-oldest ---
    for path in reversed(manifest_paths):
        if "channel-rust-nightly.toml" in path:
            continue

        if path == latest_beta or path == latest_stable:
            continue

        # Handle specific historical duplicates (keeping newest)
        current_version_match = None
        for version_str in versions_to_deduplicate:
            if version_str in path:
                current_version_match = version_str
                break

        if current_version_match:
            if current_version_match in handled_duplicates:
                log.debug(f"Skipping older duplicate: {path}")
                continue
            else:
                handled_duplicates.add(current_version_match)
                log.debug(f"Keeping newest duplicate: {path}")

        # Filter out unversioned beta releases (keep versioned ones like beta.N)
        if "beta" in path and not re.search(r"beta\.\d+", path):
            # log.debug(f"Skipping unversioned beta manifest: {path}")
            continue

        # If the path survived all filters, add it to the results.
        processed_manifests.append(path)

    log.info(
        f"Filtered manifests; {len(processed_manifests)} versioned stable/beta manifests remaining (newest first)."
    )

    # Return the cleaned list (versioned stable/beta only) and the identified latest channel paths
    return processed_manifests, latest_stable, latest_beta, latest_nightly


def parse_manifest(manifest_path: str) -> Optional[RustVersion]:
    """
    Fetches and parses a single TOML manifest file into a RustVersion object.

    Args:
        manifest_path: The relative path of the manifest (e.g., 'dist/.../file.toml').

    Returns:
        A RustVersion object if parsing is successful, None otherwise.
    """
    url = f"https://{manifest_path}"
    log.debug("Parsing manifest", url=url)
    try:
        response = requests.get(
            url, timeout=60
        )  # Longer timeout for potentially larger files
        response.raise_for_status()
        data = toml.loads(response.text)
    except requests.exceptions.RequestException as e:
        log.error("Failed to fetch manifest", url=url, error=str(e))
        return None
    except toml.TOMLDecodeError as e:
        log.error("Failed to parse TOML manifest", url=url, error=str(e))
        return None
    except KeyError as e:
        log.error("Missing expected key in manifest", url=url, key=str(e))
        return None
    except Exception as e:  # Catch unexpected errors during parsing
        log.exception("Unexpected error parsing manifest", url=url)
        return None

    try:
        release_date = data["date"]
        version_string = data["pkg"]["rustc"]["version"]
        version_match = re.match(
            r"([\d\w.-]+)", version_string
        )  # Match semver-like prefix
        if not version_match:
            log.error(
                "Could not extract version from rustc string",
                raw_version=version_string,
                url=url,
            )
            return None
        version = version_match.group(1)

        components: List[Component] = []
        for comp_name, comp_data in data.get("pkg", {}).items():
            targets: List[Target] = []
            for target_name, target_data in comp_data.get("target", {}).items():
                # Prefer xz if available, fall back to url
                pkg_url = target_data.get("xz_url", target_data.get("url"))
                pkg_hash = target_data.get("xz_hash", target_data.get("hash"))
                if pkg_url and pkg_hash:  # Ensure both URL and hash are present
                    targets.append(Target(name=target_name, url=pkg_url, hash=pkg_hash))
                else:
                    log.debug(
                        "Missing url or hash for target; skipping",
                        component=comp_name,
                        target=target_name,
                        url=url,
                    )

            if targets:
                components.append(
                    Component(
                        name=comp_name,
                        version=comp_data.get(
                            "version", "N/A"
                        ),  # Handle missing version key
                        rust_version=version,
                        git_commit=comp_data.get(
                            "git_commit_hash"
                        ),  # Safely get optional key
                        targets=targets,
                    )
                )
                log.debug(
                    f"Added component {comp_name} with {len(targets)} targets",
                    version=version,
                )

        artefacts: List[Artefact] = []
        if "artifacts" in data:
            for artefact_key, artefact_data in data["artifacts"].items():
                try:
                    # Map key like 'installer-msi' to ArtefactType.installer_msi
                    artefact_type_enum = ArtefactType[artefact_key.replace("-", "_")]
                except KeyError:
                    log.warning(
                        "Unknown artifact type in manifest", type=artefact_key, url=url
                    )
                    continue

                for target_name, target_list in artefact_data.get("target", {}).items():
                    if target_list:  # List should contain dicts
                        artefact_info = target_list[
                            0
                        ]  # Assume first entry is the relevant one
                        art_url = artefact_info.get("url")
                        art_hash = artefact_info.get("hash-sha256")
                        if art_url and art_hash:
                            artefacts.append(
                                Artefact(
                                    type=artefact_type_enum,
                                    url=art_url,
                                    hash=art_hash,
                                    target=target_name,
                                )
                            )
                        else:
                            log.warning(
                                "Missing url or hash for artifact",
                                type=artefact_key,
                                target=target_name,
                                url=url,
                            )

        renames = data.get("renames")
        profiles = data.get("profiles")

        return RustVersion(
            version=version,
            release_date=release_date,
            manifest_url=url,  # Store the full URL
            components=components,
            profiles=profiles,
            renames=renames,
            artefacts=artefacts,
            # latest_* flags are set later based on channel manifests
            latest_nightly=(
                "nightly" in manifest_path and "channel" in manifest_path
            ),  # Check if it's the channel manifest
        )
    except Exception as e:  # Catch unexpected errors during data extraction
        log.exception("Unexpected error extracting data from parsed manifest", url=url)
        return None


def set_rust_channel_flags(
    db_connection: sqlite3.Connection,
    stable_manifest_path: Optional[str],
    beta_manifest_path: Optional[str],
    nightly_manifest_path: Optional[str],
    parsed_versions: Dict[str, RustVersion],  # Map manifest path to parsed version
):
    """
    Updates the `latest_stable`, `latest_beta`, and `latest_nightly` flags
    for the corresponding Rust versions in the database.

    Args:
        db_connection: The active SQLite database connection.
        stable_manifest_path: Path of the latest stable channel manifest.
        beta_manifest_path: Path of the latest beta channel manifest.
        nightly_manifest_path: Path of the latest nightly channel manifest.
        parsed_versions: Dictionary mapping manifest paths to successfully parsed RustVersion objects.
    """
    stable_version = None
    beta_version = None
    nightly_version = None

    # Parse stable and beta manifests directly as they are not pre-parsed
    if stable_manifest_path:
        log.info("Parsing stable channel manifest.", path=stable_manifest_path)
        parsed = parse_manifest(stable_manifest_path)
        if parsed:
            stable_version = parsed.version
        else:
            log.warning(
                "Failed to parse stable channel manifest.", path=stable_manifest_path
            )

    if beta_manifest_path:
        log.info("Parsing beta channel manifest.", path=beta_manifest_path)
        parsed = parse_manifest(beta_manifest_path)
        if parsed:
            beta_version = parsed.version
        else:
            log.warning(
                "Failed to parse beta channel manifest.", path=beta_manifest_path
            )

    # TODO: We should be able to just parse the nightly object but need to match
    #static.rust-lang.org/dist/2025-03-29/channel-rust-nightly.toml
    #{'dist/2025-03-29/channel-rust-nightly.toml': RustVersion(version='1.87.0-nightly',

    if nightly_manifest_path:
        log.info("Parsing nightly channel manifest.", path=nightly_manifest_path)
        parsed = parse_manifest(nightly_manifest_path)
        if parsed:
            nightly_version = parsed.version
        else:
            log.warning(
                "Failed to parse beta channel manifest.", path=nightly_manifest_path
        )

    log.info(
        "Setting channel flags in database",
        latest_stable=stable_version,
        latest_beta=beta_version,
        latest_nightly=nightly_version,
    )

    sq.set_rust_channel_flags(
        db_connection, stable_version, beta_version, nightly_version
    )


def get_versions_to_process(
    manifest_paths: list[str], db_connection: sqlite3.Connection
) -> list[str]:
    """
    Filters the list of manifest paths, removing those whose corresponding
    versions are already present in the database.

    Args:
        manifest_paths: The list of manifest paths to potentially process.
        db_connection: The active SQLite database connection.

    Returns:
        list[str]: The list of manifest paths that need to be processed.
    """
    log.info("Checking database for existing versions...")

    manifest_versions: Dict[str, str] = {}  # path -> version
    versions_to_check: List[str] = []
    version_pattern = re.compile(r"channel-rust-([\d\w.-]+?)(?:-beta(?:\.\d+)?)?\.toml")

    for path in manifest_paths:
        match = version_pattern.search(path)
        if match:
            version = match.group(1)
            # Include beta suffix in the version identifier if present
            # Example: 1.78.0-beta.1 from channel-rust-1.78.0-beta.1.toml
            if "-beta" in match.group(0):  # Check original path string for '-beta' part
                beta_match = re.search(r"-(beta(?:\.\d+)?)", match.group(0))
                if beta_match:
                    version = f"{version}-{beta_match.group(1)}"  # e.g., 1.78.0-beta.1

            manifest_versions[path] = version
            versions_to_check.append(version)
        else:
            log.warning("Could not extract version from manifest path", path=path)

    if not versions_to_check:
        log.info("No valid versioned manifests found to check against database.")
        return []

    existing_versions = sq.get_existing_versions(db_connection, versions_to_check)

    log.info(
        f"Found {len(existing_versions)} matching versions already in the database."
    )

    new_manifests = []
    processed_count = 0
    skipped_count = 0
    for path, version in manifest_versions.items():
        if version not in existing_versions:
            log.debug(f"Version '{version}' needs processing.", path=path)
            new_manifests.append(path)
            processed_count += 1
        else:
            # Log skipped versions less verbosely, maybe sample a few
            if skipped_count < 5 or skipped_count % 100 == 0:
                log.debug(f"Version '{version}' already exists, skipping.", path=path)
            skipped_count += 1

    if skipped_count > 5:
        log.debug(f"...skipped {skipped_count - 5} more existing versions.")

    log.info(f"Identified {processed_count} new manifests to process.")
    return new_manifests


def main():
    parser = argparse.ArgumentParser(
        description="Ingest Rust release information into an SQLite database.",
        epilog="Fetches manifests, parses them, and stores version/component/target info.",
    )
    parser.add_argument(
        "--number",
        type=int,
        default=0,
        help="Process only the first N new/updated items (excluding nightly) for testing.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {VERSION}")
    parser.add_argument(
        "--database",
        type=str,
        default=DEFAULT_DB_PATH,
        help=f"Path to the SQLite database file. Defaults to '{DEFAULT_DB_PATH}'.",
    )
    parser.add_argument(
        "--force-update",
        action="store_true",
        help="Force reprocessing of all versions, even if they exist in the database.",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=None,
        help="Maximum number of worker threads for fetching/parsing manifests.",
    )

    args = parser.parse_args()
    db_path = os.path.abspath(args.database)
    max_workers = args.max_workers
    db_connection = None

    try:
        # --- Database Setup ---
        is_new_db = not os.path.exists(db_path)
        db_connection = sqlite3.connect(db_path, timeout=10)
        log.info("Database connection established", path=db_path)
        # Enable foreign key support
        db_connection.execute("PRAGMA foreign_keys = ON;")

        if is_new_db:
            log.info("Database file does not exist. Initializing schema.")
            sq.init_tables(db_connection) 
        else:
            log.info("Database file exists. Will update.")
            # Schema migration checks could go here

        # --- Fetch and Filter Manifests ---
        all_manifest_paths = fetch_manifest_list()
        log.info(f"Found {len(all_manifest_paths)} total manifest paths on dist server")
        filtered_paths, stable_path, beta_path, nightly_path = (
            filter_and_sort_manifests(all_manifest_paths)
        )
        log.info(
            f"Processing {len(filtered_paths)} filtered/sorted manifests (versioned stable/beta only)."
        )

        # --- Identify Manifests to Process ---
        manifests_to_parse_candidates = []
        if args.force_update:
            log.warning("Forcing update: all filtered manifests will be processed.")
            manifests_to_parse_candidates = filtered_paths
        elif is_new_db:
            log.info("New database, processing all filtered manifests.")
            manifests_to_parse_candidates = filtered_paths
        else:
            # Get versioned stable/beta manifests not already in DB
            manifests_to_parse_candidates = get_versions_to_process(
                filtered_paths, db_connection
            )

        # We always need to include nightly, but 'stable' and 'beta' will clash with the actual channel manifests
        # that they represent.
        manifests_to_parse = list(manifests_to_parse_candidates)  # Create mutable list
        if nightly_path:
            manifests_to_parse.insert(0, nightly_path)

        if args.number > 0:
            log.info(
                f"Limiting processing to {args.number} manifests based on --number flag."
            )
            # Apply limit *after* adding essential channel manifests
            manifests_to_parse = manifests_to_parse[: args.number]

        if not manifests_to_parse:
            log.info("No new or required manifests to process.")
            # If needed, logic to ensure flags are still set correctly even if no versions were added
            # could go here, potentially parsing channels explicitly if manifests_to_parse is empty.
            # For now, assume if nothing to parse, flags are likely up-to-date unless forced.
            return  # Exit cleanly

        # --- Parse Manifests Concurrently ---
        log.info(
            f"Parsing {len(manifests_to_parse)} manifests using up to {max_workers or 'default'} workers..."
        )
        parsed_rustversions: List[RustVersion] = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = list(
                tqdm(
                    executor.map(parse_manifest, manifests_to_parse),
                    total=len(manifests_to_parse),
                    desc="Parsing Manifests",
                )
            )
            parsed_rustversions = [result for result in results if result is not None]
            log.info(f"Successfully parsed {len(parsed_rustversions)} manifests.")

        if not parsed_rustversions:
            log.error("No manifests could be successfully parsed. Exiting.")
            return

        # --- Prepare for Database Insertion ---
        parsed_versions_map = {
            rv.manifest_url.replace(f"{DIST_SERVER_BASE_URL}/", ""): rv
            for rv in parsed_rustversions
        }

        # Check for duplicate versions post-parsing (should be less likely now)
        versions_found: Dict[str, str] = {}
        unique_versions_to_insert: List[RustVersion] = []
        for rv in parsed_rustversions:
            if rv.version in versions_found:
                log.warning(
                    "Duplicate version detected after parsing",
                    version=rv.version,
                    manifest1=versions_found[rv.version],
                    manifest2=rv.manifest_url,
                )
                # Keeping the last one parsed (likely from a later manifest if versions clash unexpectedly)
                # Find and remove previous entry from unique_versions_to_insert if necessary
                unique_versions_to_insert = [
                    v for v in unique_versions_to_insert if v.version != rv.version
                ]
            versions_found[rv.version] = rv.manifest_url
            unique_versions_to_insert.append(rv)

        log.info(
            f"Prepared {len(unique_versions_to_insert)} unique Rust versions for database insertion/update."
        )

        # --- Insert/Update Database ---

        # ** Delete old nightly data BEFORE inserting new data **
        # Find the new nightly object from the parsed results
        nightly_obj = next(
            (rv for rv in unique_versions_to_insert if rv.latest_nightly), None
        )

        if not is_new_db:
            if nightly_obj:
                log.info(
                    f"New nightly version {nightly_obj.version} identified. Deleting previous nightly data..."
                )
                try:
                    deleted_count = sq.delete_nightly(db_connection)
                    log.info(
                        f"Deletion of previous nightly data complete (affected {deleted_count} rows in rust_versions)."
                    )
                except sqlite3.Error as e:
                    log.error(
                        "Failed during deletion of previous nightly data.",
                        error=str(e),
                        exc_info=True,
                    )
                    db_connection.rollback()
                    sys.exit("Error deleting previous nightly data.")
            else:
                log.info("No new nightly channel manifest parsed in this batch.")

        inserted_count = 0
        log.info(
            f"Processing {len(unique_versions_to_insert)} versions for insertion/update..."
        )
        for version in tqdm(unique_versions_to_insert, desc="Inserting Versions"):
            log.debug(f"Inserting/updating version {version.version}")
            try:
                sq.insert_rust_version(db_connection, version)
                inserted_count += 1
            except sqlite3.IntegrityError as e:
                # This might happen if force-update is used, or if a version exists despite filtering (unlikely)
                # Or if UNIQUE constraints are violated (e.g., duplicate URL)
                log.error(
                    f"Integrity error inserting version {version.version}. It might already exist or violate a constraint.",
                    error=str(e),
                )
            except Exception as e:
                log.exception(
                    f"Failed to insert version {version.version} into database."
                )

        log.info(f"Finished database insertions/updates for {inserted_count} versions.")

        # --- Set Channel Flags ---
        # This should run *after* all inserts/deletes are done in the transaction
        set_rust_channel_flags(
            db_connection, stable_path, beta_path, nightly_path, parsed_versions_map
        )

        # --- Commit changes ---
        log.info("Committing database transaction.")
        db_connection.commit()

    except sqlite3.Error as e:
        log.exception("An SQLite error occurred", db_path=db_path)
        if db_connection:
            log.warning("Rolling back database changes due to SQLite error.")
            db_connection.rollback()
    except Exception as e:
        log.exception("An unexpected error occurred during execution.")
        if db_connection:
            log.warning("Rolling back database changes due to unexpected error.")
            db_connection.rollback()
    finally:
        if db_connection:
            log.info("Closing database connection.")
            db_connection.close()


if __name__ == "__main__":
    main()
