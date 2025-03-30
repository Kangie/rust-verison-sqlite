#!/usr/bin/env python3

# This script is used to ingest rust release information into an sqlite database for later querying.

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from enum import Enum
from tqdm.contrib.concurrent import process_map
from typing import List, Dict, Optional, Tuple
import argparse
import helpers.sqlite as sq
import os
import re
import requests
import sqlite3
import structlog
import tomllib as toml

loglevel = os.environ.get('LOGLEVEL', 'INFO').upper()
structlog.configure(wrapper_class=structlog.make_filtering_bound_logger(loglevel))
log = structlog.get_logger()

# High level overview of the script:
# 1. fetch the list of manifests from the rust-lang dist server
# https://static.rust-lang.org/manifests.txt This contains a list
# of toml manifests for every rust release (including every nightly!)
# 2. Strip out nightly (we'll keep exactly the latest nightly) and non-versioned beta releases
# 3. Identify versions in the manifest files
# 4. Compare the versions to what we have in our database
# 5. Query the toml files for the release info, in particular we want:
#    - release date
#    - version (we can get this and the above from the filename, but it's here anyway)
#    - packages
#    - targets for each package
#    - target URLs (let's just do xz),
#    - Their hashes
# 6. For a given version we also want the available 'profiles' and any rename information.


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
    git_commit: str
    targets: List[Target]


@dataclass
class ArtefactType(Enum):
    installer_msi = 1
    installer_pkg = 2
    source_code = 3


@dataclass
class Artefact:
    type: ArtefactType
    url: str
    hash: str


@dataclass
class RustVersion:
    version: str
    release_date: str
    components: List[Component]
    profiles: Optional[Dict[str, List[str]]] = None
    renames: Optional[Dict[str, str]] = None
    artefacts: Optional[List[Artefact]] = None
    latest_stable: bool = False
    latest_beta: bool = False
    latest_nightly: bool = False
    manifest: Optional[str] = None


def fetch_manifests():
    """
    Fetches the list of Rust manifests from the official Rust static URL.

    Returns:
        list: A list of strings, where each string is a line from the manifests file.

    Raises:
        Exception: If the HTTP request to fetch the manifests fails.
    """
    url = 'https://static.rust-lang.org/manifests.txt'
    log.debug(f'Fetching manifests from {url}')
    r = requests.get(url)
    if r.status_code != 200:
        raise Exception('Failed to fetch manifests')
    return r.text.split('\n')


def remove_old_channel_updates(manifests: List[str]) -> Tuple[List[str], str, str, str]:
    """
    Removes nightly and duplicate beta releases from the list of manifests.

    Args:
        manifests (list): A list of strings, where each string is Rust manifest

    Returns:
        list: A list of strings with all all manifested versions (and nightly!).
        str: The latest stable version.
        str: The latest beta version.
        str: The latest nightly version.
    """

    latest_nightly = None
    latest_beta = None
    latest_stable = None
    filtered_manifests = []

    # Some old manifests are duplicated, we'll filter out the older of the two
    count_1_8 = 0
    count_1_14 = 0
    count_1_15 = 0
    count_1_49 = 0

    for manifest in manifests:
        if not manifest:
            continue
        match manifest:
            case _ if 'channel-rust-nightly.toml' in manifest:
                latest_nightly = manifest
            case _ if 'channel-rust-beta.toml' in manifest:
                latest_beta = manifest
            case _ if 'channel-rust-stable.toml' in manifest:
                latest_stable = manifest
            case _ if '1.8.0' in manifest and count_1_8 < 1:
                count_1_8 += 1
            case _ if '1.14.0' in manifest and count_1_14 < 1:
                count_1_14 += 1
            case _ if '1.15.1' in manifest and count_1_15 < 1:
                count_1_15 += 1
            case _ if '1.49.0' in manifest and count_1_49 < 2:
                count_1_49 += 1
            case _ if 'beta' in manifest:
                if re.search(r'beta\.\d', manifest):
                    filtered_manifests.append(manifest)
            case _ if manifest.count('.') <= 4:
                continue
            case _:
                filtered_manifests.append(manifest)

    filtered_manifests.append(latest_nightly)

    log.debug(f"Removed duplicate \"channel\" releases; {len(filtered_manifests)} manifests remaining")

    # We want to process newest to oldest, so reverse the list before returning
    return filtered_manifests[::-1], latest_stable, latest_beta, latest_nightly


def parse_manifest(manifest: str) -> RustVersion:
    """
    Parses the manifest data and extracts Rust version information.

    Args:
        data (Dict): A dictionary containing the manifest data. Expected keys include:
            - 'date': The release date of the Rust version.
            - 'pkg': A dictionary containing package information, including 'rustc' version and other components.
            - 'artifacts': A dictionary of artifact types and their associated data.
            - Optional keys: 'renames', 'profiles'.

    Returns:
        RustVersion: An object representing the parsed Rust version, including:
            - version (str): The Rust version string.
            - release_date (str): The release date of the Rust version.
            - components (List[Component]): A list of components (e.g., cargo, clippy, rustc) with their respective targets.
            - profiles (Optional): Profiles data if available in the manifest.
            - renames (Optional): Renames data if available in the manifest.
            - artefacts (List[Artefact]): A list of artefacts associated with the Rust version.

    Raises:
        KeyError: If required keys are missing in the input data.
        ValueError: If the data structure does not match the expected format.
    """
    url = f"https://{manifest}"
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch manifest from {url}")

    log.debug(f"Parsing manifest: {url}")
    data = toml.loads(response.text)

    release_date = data['date']
    version = data['pkg']['rustc']['version'].split(' ')[0]

    # cargo, clippy, rustc, etc
    components: List[Component] = []
    for component in data['pkg']:
        targets: List[Target] = []
        for target in data['pkg'][component]['target']:
            # We'll prefer xz if available
            url = data['pkg'][component]['target'][target].get('xz_url',
                                                               data['pkg'][component]['target'][target].get('url'))
            hash_value = data['pkg'][component]['target'][target].get('xz_hash',
                                                                      data['pkg'][component]['target'][target].get('hash'))
            targets.append(Target(target, url, hash_value))
        log.debug(f"Adding component {component} with {len(targets)} targets")
        components.append(Component(
            component,
            data['pkg'][component]['version'],
            version,  # We'll match this to the overall Rust version
            data['pkg'][component].get('git_commit_hash', None),  # Default to None if not present (miri on non-nightly, for example)
            targets
        ))
    artefacts = []
    if 'artifacts' in data:
        for artefact_type in data['artifacts']:
            for target in data['artifacts'][artefact_type]['target']:
                # This is a list, though we really only expect one item per target at this point.
                artefacts.append(Artefact(
                    ArtefactType[artefact_type.replace('-', '_')].value,
                    data['artifacts'][artefact_type]['target'][target][0]['url'],
                    data['artifacts'][artefact_type]['target'][target][0]['hash-sha256']
                ))

    renames = data.get('renames')
    profiles = data.get('profiles')

    return RustVersion(version, release_date, components, profiles, renames, artefacts, url)


def update_rust_version_flags(rusts: List[RustVersion], stable: str, beta: str, nightly: str):
    """
    Updates the `latest_stable`, `latest_beta`, and `latest_nightly` flags for the given Rust versions.

    Args:
        rusts (List[RustVersion]): A list of RustVersion objects.
        stable (str): The latest stable version.
        beta (str): The latest beta version.
        nightly (str): The latest nightly version.
    """

    stable = parse_manifest(stable).version
    beta = parse_manifest(beta).version
    nightly = parse_manifest(nightly).version

    for rust in rusts:
        rust.latest_stable = rust.version == stable
        rust.latest_beta = rust.version == beta
        rust.latest_nightly = rust.version == nightly


def main():
    parser = argparse.ArgumentParser(
        description="Ingest Rust release information into an SQLite database."
    )
    parser.add_argument(
        "--number",
        type=int,
        default=0,
        help="Process only the first N items for testing purposes."
    )
    parser.add_argument(
        "--version",
        action="version",
        version="rust-version-parser 0.1",
        help="Show program's version number and exit."
    )
    parser.add_argument(
        "--database",
        type=str,
        default="./rust_versions.sqlite3",
        help="Path to the SQLite database file. Defaults to './rust_versions.sqlite3'."
    )

    # Parse arguments
    args = parser.parse_args()
    database = os.path.abspath(args.database)

    if not os.path.exists(database):
        log.info(f"Database file '{database}' does not exist. Creating a new one.")
        with sqlite3.connect(database) as conn:
            pass  # This will create an empty database file
        sq.init_tables(database)

    manifests = fetch_manifests()
    log.info(f"Found {len(manifests)} manifests on dist server")
    clean_manifests, stable, beta, nightly = remove_old_channel_updates(manifests)
    log.info(f"Processing {len(clean_manifests)} manifests after removing old channel updates")

    if args.number > 0:
        log.info(f"Truncating manifest list to {args.number} items")
        clean_manifests = clean_manifests[:args.number]

    # The limiting factor here is doing single-threaded HTTP requests to fetch the manifests.
    # We can parallelise this with ThreadPoolExecutor.
    with ThreadPoolExecutor():
        rustversions: List[RustVersion] = process_map(parse_manifest, clean_manifests)

    update_rust_version_flags(rustversions, stable, beta, nightly)

    # Log versions in batches
    batch_size = 6
    version_batches = [rustversions[i:i + batch_size] for i in range(0, len(rustversions), batch_size)]
    for batch in version_batches:
        log.debug("Versions: " + ", ".join([version.version for version in batch]))

    # Check for duplicate versions (sanity check for development, mostly)
    seen_versions = {}
    for version in rustversions:
        if version.version in seen_versions:
            raise ValueError(
                f"Duplicate version detected: {version.version}. "
                f"First occurrence URL: {seen_versions[version.version]}, "
                f"Duplicate occurrence URL: {version.manifest}"
            )
        seen_versions[version.version] = version.manifest

    for version in rustversions:
        sq.insert_rust_version(database, version)


if __name__ == "__main__":
    main()
