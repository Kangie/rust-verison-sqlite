from rust_version_parser import log, RustVersion
import semver
import sqlite3


def execute_query(database: str, query: str) -> bool:
    '''
    Open a connection to our database, execute our query in a transaction
    and output some useful logging information, then neatly close the connection.
    Returns True if the transaction was successful, otherwise raises an exception.
    '''
    table = None
    type = None
    match query:
        case q if 'INSERT INTO' in q:
            table = q.split('INSERT INTO')[1].split()[0]
            type = 'insert'
        case q if 'UPDATE' in q:
            table = q.split('UPDATE')[1].split()[0]
            type = 'update'
        case q if 'CREATE TABLE' in q:
            table = q.split('CREATE TABLE')[1].split()[0]
            type = 'create'
        case q if 'DELETE FROM' in q:
            table = q.split('DELETE FROM')[1].split()[0]
            type = 'delete'
        case _:
            raise ValueError("Query does not support this operation")

    try:
        connection = sqlite3.connect(database)
        cursor = connection.cursor()
        log.debug(f'Connected to {database}')
        cursor.execute(query)
        connection.commit()
        match type:
            case 'insert':
                log.debug(f'Inserted {cursor.rowcount} rows into {table}')
            case 'update':
                log.debug(f'Updated {cursor.rowcount} rows in {table}')
            case 'create':
                log.debug(f'Created table {table}')
            case 'delete':
                log.debug(f'Deleted {cursor.rowcount} rows from {table}')
        cursor.close()
        return True

    except sqlite3.Error as error:
        f = open(f'./{table}_{type}_query.sql', "w")
        f.write(query)
        f.close()
        raise Exception(f'Failed to execute query on {table}', error)
    finally:
        if connection:
            connection.close()
            log.debug(f'Closed connection to {database}')


def generate_insert(tablename: str, columns: tuple, rows: list[tuple]) -> str:
    '''
    Generate a SQLite insert statement for inputs:
    table_name, ('columnx', ... 'columny'), [(column_content,...)]
    '''
    insert: list[str] = []
    insert.append(f'INSERT INTO {tablename} ({",".join(columns)})')
    insert.append("VALUES")
    for idx, row in enumerate(rows):
        eol = ";" if idx == len(rows)-1 else ','
        content: list[str] = []
        # Turn our values into something that SQL will like!
        for value in row:
            # No quotes
            if isinstance(value, int):
                content.append(value)
            elif isinstance(value, str) and value.isnumeric():
                content.append(value)
            elif value == "NULL":
                content.append("NULL")
            else:
            # Quotes!
                content.append(f"\"{value}\"")
        insert.append(f"\t({','.join(content)}){eol}")
    return '\n'.join(insert)


def init_tables(database):
    """
    Initialises the tables in the given SQLite database.

    This function creates a set of predefined tables in the SQLite database.
    Each table is defined with its name, columns, and optional constraints
    such as primary keys and foreign keys. The function generates SQL
    `CREATE TABLE` statements dynamically based on the table definitions
    and executes them.

    Args:
        database: The SQLite database connection object where the tables
                  will be created.

    """
    '''
    Initialise tables
    '''
    tables = [
        {
            'name': 'rust_versions',
            'columns': [
                {'name': 'version', 'type': 'TEXT', 'primary_key': True},
                {'name': 'release_date', 'type': 'INTEGER'},
                {'name': 'latest_stable', 'type': 'INTEGER', 'default': 0},
                {'name': 'latest_beta', 'type': 'INTEGER', 'default': 0},
                {'name': 'latest_nightly', 'type': 'INTEGER', 'default': 0},
            ]
        },
        {
            'name': 'components',
            'columns': [
                {'name': 'name', 'type': 'TEXT'},
                {'name': 'id', 'type': 'INTEGER', 'primary_key': True},
                {'name': 'version', 'type': 'TEXT'},
                {'name': 'rust_version', 'type': 'TEXT'},
                {'name': 'git_commit', 'type': 'TEXT'},
                {'name': 'profile_complete', 'type': 'INTEGER', 'default': 0},
                {'name': 'profile_default', 'type': 'INTEGER', 'default': 0},
                {'name': 'profile_minimal', 'type': 'INTEGER', 'default': 0},
            ],
            'constraints': {
                'foreign_key': {
                    'name': 'rust_version',
                    'table': 'rust_versions',
                    'references': 'version',
                }
            }
        },
        {
            'name': 'targets',
            'columns': [
                {'name': 'name', 'type': 'TEXT'},
                {'name': 'id', 'type': 'INTEGER', 'primary_key': True},
                {'name': 'url', 'type': 'TEXT'},
                {'name': 'hash', 'type': 'TEXT'},
                {'name': 'component', 'type': 'INTEGER'},
            ],
            'constraints': {
                'foreign_key': {
                    'name': 'component',
                    'table': 'components',
                    'references': 'id',
                }
            }
        },
        {
            'name': 'artefacts',
            'columns': [
                {'name': 'id', 'type': 'INTEGER', 'primary_key': True},
                {'name': 'rust_version', 'type': 'TEXT'},
                {'name': 'type', 'type': 'INTEGER'},
                {'name': 'url', 'type': 'TEXT'},
                {'name': 'hash', 'type': 'TEXT'},
            ],
            'constraints': {
                'foreign_key': {
                    'name': 'rust_version',
                    'table': 'rust_versions',
                    'references': 'version',
                }
            }
        },
    ]
    for table in tables:
        statement: list = []
        statement.append(f'CREATE TABLE {table["name"]} (\n')
        for column in table["columns"]:
            # column_name data_type [PRIMARY KEY, NOT NULL, DEFAULT 0, etc.]
            statement.append(f'\t{column["name"]} {column["type"]}')
            if "primary_key" in column:
                statement.append(' PRIMARY KEY,\n')
            elif "unique" in column:
                statement.append(' UNIQUE,\n')
            else:
                statement.append(',\n')
        if "constraints" in table:
            # There are other constraints, but this is all I care about for now
            if "foreign_key" in table["constraints"]:
                statement.append(f'\tFOREIGN KEY ({table["constraints"]["foreign_key"]["name"]})\n')
                statement.append(f'\t\tREFERENCES {table["constraints"]["foreign_key"]["table"]} ({table["constraints"]["foreign_key"]["references"]})\n')
        # Tidy up the last line of the statement
        lastrow = statement[-1].replace(',\n', '\n')
        del statement[-1]
        statement.append(lastrow)
        statement.append(");")
        execute_query(database, ''.join(statement))


def get_id_for_component(database: str, component_name: str, rust_version: str) -> int:
    """
    Retrieve the ID of a component from the database.

    Args:
        database (str): The file path to the SQLite database.
        component_name (str): The name of the component to retrieve.
        rust_version (str): The version of Rust for which the component is associated.

    Returns:
        int: The ID of the component if found, otherwise -1.

    Raises:
        sqlite3.Error: If an error occurs while connecting to the database or executing the query.
    """

    try:
        connection = sqlite3.connect(database)
        cursor = connection.cursor()
        cursor.execute(f'''
                       SELECT
                        id
                       FROM
                        components
                       WHERE
                        name = "{component_name}"
                       AND
                        rust_version = "{rust_version}";
                    ''')
        record = cursor.fetchone()
        cursor.close()

        if record:
            return record[0]
        else:
            return -1

    except sqlite3.Error as error:
        log.error(f'Failed to read data from components', error)
        return -1

    finally:
        if connection:
            connection.close()
            log.debug(f'Closed connection to {database}')


def insert_rust_version(database: str, rust: RustVersion) -> bool:
    """
    Insert a Rust version into the database.
    """

    log.info(f'Inserting Rust version {rust.version} into the database')
    execute_query(
        database,
        generate_insert(
            'rust_versions',
            ('version', 'release_date', 'latest_stable', 'latest_beta', 'latest_nightly'),
            [(rust.version, rust.release_date, str(int(rust.latest_stable)), str(int(rust.latest_beta)), str(int(rust.latest_nightly)))]
        )
    )

    log.info(f'Inserting components for Rust {rust.version} into the database')
    execute_query(
        database,
        generate_insert(
            'components',
            ('name', 'version', 'rust_version', 'git_commit', 'profile_complete', 'profile_default', 'profile_minimal'),
            [(component.name, component.version,
              component.rust_version, component.git_commit, '0', '0', '0') for component in rust.components]
        )
    )

    for component in rust.components:
        log.info(f'Inserting targets for Rust {rust.version} component: {component.name} into the database')

        component_id = get_id_for_component(database, component.name, rust.version)
        if component_id == -1:
            log.error(f'Failed to retrieve ID for component {component.name}')
            raise Exception(f'Component ID not found for {component.name}')

        target_rows = [
            (target.name, target.url, target.hash, str(component_id))
            for target in component.targets if target.url  # Filter out targets with None or empty URLs
        ]

        if len(target_rows) > 0:
            execute_query(
                database,
                generate_insert(
                    'targets',
                    ('name', 'url', 'hash', 'component'),
                    target_rows
                )
            )

    log.info(f'Inserting artefacts for Rust version {rust.version} into the database')
    artefact_rows = [
        (rust.version, str(artefact.type), artefact.url, artefact.hash)
        for artefact in rust.artefacts
    ]
    if artefact_rows:
        execute_query(
            database,
            generate_insert(
                'artefacts',
                ('rust_version', 'type', 'url', 'hash'),
                artefact_rows
            )
        )

    if semver.compare(rust.version, '1.32.0') > 0:
        log.info(f'Marking Rust {rust.version} components with profile flags')

        for profile, components in rust.profiles.items():
            query = f'''
            UPDATE components SET profile_{profile} = 1
            WHERE name IN ({",".join(f"\"{component}\"" for component in components)})
            AND rust_version = "{rust.version}";
            '''
            execute_query(database, query)
