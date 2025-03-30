######
README
######

This repository contains two components:

- A python script to read manifests from the Rust Distribution server ingest the information in a SQLite database.
- A Rust-based web app to serve the information from the SQLite database into templated pages.

That is all.


Usage
=====

To ingest the information from the Rust Distribution server, run the following command:

.. code-block:: console

    user@host$ python3 rust_manifest_ingestor.py

This will create a SQLite database file named `rust_versions.db` in the root of the repository.

To run the web app, run the following command:

.. code-block:: console

    user@host$ cargo run

The web app will launch on port 8080. You can access the following pages:

- `/`
- `/info/all`
- `/info/<version>`

Where `<version>` is a version number, such as `1.0.0`, `1.0.0-beta`, or `1.0.0-nightly`, or `nightly`, `beta`, or `stable`.
