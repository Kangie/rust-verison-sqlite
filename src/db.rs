use actix_web::{error, web, Error};
use rusqlite::Statement;

use crate::models::{Component, ComponentTarget, RustVersion};

pub type Pool = r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>;
pub type Connection = r2d2::PooledConnection<r2d2_sqlite::SqliteConnectionManager>;
type RustVersionsAggResult = Result<Vec<RustVersion>, rusqlite::Error>;
type ComponentAggResult = Result<Vec<Component>, rusqlite::Error>;

#[allow(clippy::enum_variant_names)]
pub enum Queries {
    GetNamedChannels,
    GetAllVersions,
    GetVersionInfo,
}

pub async fn execute(pool: &Pool, query: Queries, param: Option<String>) -> Result<Vec<RustVersion>, Error> {
    let pool = pool.clone();

    let conn = web::block(move || pool.get())
        .await?
        .map_err(error::ErrorInternalServerError)?;

    web::block(move || {
        match query {
            Queries::GetNamedChannels => get_named_channels(&conn),
            Queries::GetAllVersions => get_all_versions(&conn),
            Queries::GetVersionInfo => get_version_info(&conn, param),
        }
    })
    .await?
    .map_err(error::ErrorInternalServerError)
}

fn get_named_channels(conn: &Connection) -> RustVersionsAggResult {
    let stmt = conn.prepare(
        "SELECT
            version, release_date, latest_stable, latest_beta, latest_nightly
        FROM
            rust_versions
        WHERE
            latest_stable = 1 OR latest_beta = 1 OR latest_nightly = 1
        ORDER BY
            release_date
        DESC LIMIT 3",
    )?;

    get_named_channel_rows(stmt)
}

fn get_named_channel_rows(mut statement: Statement) -> RustVersionsAggResult {
    statement
        .query_map([], |row| {
            Ok(RustVersion {
                version: row.get(0)?,
                release_date: row.get(1)?,
                latest_stable: row.get(2)?,
                latest_beta: row.get(3)?,
                latest_nightly: row.get(4)?,
                components: vec![],
                profiles: None,
                renames: None,
                artefacts: None,
            })
        })
        .and_then(Iterator::collect)
}

fn get_rust_components(conn: &Connection, version: &str) -> ComponentAggResult {
    let stmt = conn.prepare(
        "SELECT
            components.name, components.version, components.git_commit, components.profile_complete,
            components.profile_default, components.profile_minimal, targets.name, targets.url,
            targets.hash
        FROM
            components
        LEFT JOIN
            targets
        ON
            components.id = targets.component
        WHERE
            components.rust_version = ?1",
    )?;

    get_component_rows(stmt, version)
}

fn get_component_rows(mut statement: Statement, version: &str) -> ComponentAggResult {
    let mut components_map: std::collections::HashMap<String, Component> = std::collections::HashMap::new();

    let _rows: Vec<_> = statement.query_map([version], |row| {
        let name: String = row.get("name")?;
        let target = if let (Ok(name), Ok(url), Ok(hash)) = (
            row.get::<_, Option<String>>("targets.name"),
            row.get::<_, Option<String>>("targets.url"),
            row.get::<_, Option<String>>("targets.hash"),
        ) {
            Some(ComponentTarget {
            name: name.unwrap_or_default(),
            url: url.unwrap_or_default(),
            hash: hash.unwrap_or_default(),
            })
        } else {
            None
        };

        if let Some(component) = components_map.get_mut(&name) {
            if let Some(target) = target {
            if let Some(targets) = &mut component.target {
                targets.push(target);
            } else {
                component.target = Some(vec![target]);
            }
            }
        } else {
            components_map.insert(
            name.clone(),
            Component {
                name: row.get("name")?,
                version: row.get("version")?,
                git_commit: row.get("git_commit")?,
                profile_complete: row.get("profile_complete")?,
                profile_default: row.get("profile_default")?,
                profile_minimal: row.get("profile_minimal")?,
                target: target.map(|t| vec![t]),
            },
            );
        }

        Ok(())
    })?.collect::<Result<_, rusqlite::Error>>()?;

    print!("{:?}", components_map);

    Ok(components_map.into_values().collect())
}

fn get_all_versions(conn: &Connection) -> RustVersionsAggResult {
    let stmt = conn.prepare(
        "SELECT
            version, release_date, latest_stable, latest_beta, latest_nightly
        FROM
            rust_versions
        ORDER BY
            release_date
        DESC",
    )?;

    get_all_version_rows(stmt)
}

fn get_all_version_rows(mut statement: Statement) -> RustVersionsAggResult {
    statement
        .query_map([], |row| {
            Ok(RustVersion {
                version: row.get(0)?,
                release_date: row.get(1)?,
                latest_stable: row.get(2)?,
                latest_beta: row.get(3)?,
                latest_nightly: row.get(4)?,
                components: vec![],
                profiles: None,
                renames: None,
                artefacts: None,
            })
        })
        .and_then(Iterator::collect)
}

fn get_version_info(conn: &Connection, version: Option<String>) -> RustVersionsAggResult {
    let version_str = version.as_deref().unwrap_or("latest");

    let query_version = match get_all_versions(conn) {
        Ok(versions) => match version_str {
            "latest" | "stable" => versions
                .iter()
                .find(|v| v.latest_stable)
                .map(|v| v.version.clone())
                .ok_or_else(|| rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "No stable version found")))),
            "beta" => versions
                .iter()
                .find(|v| v.latest_beta)
                .map(|v| v.version.clone())
                .ok_or_else(|| rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "No beta version found")))),
            "nightly" => versions
                .iter()
                .find(|v| v.latest_nightly)
                .map(|v| v.version.clone())
                .ok_or_else(|| rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "No nightly version found")))),
            _ => versions
                .iter()
                .find(|v| v.version == version_str)
                .map(|v| v.version.clone())
                .ok_or_else(|| rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "Version not found")))),
        },
        Err(e) => Err(rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::new(std::io::ErrorKind::Other, e)))),
    }?;

    let stmt = conn.prepare(
        "SELECT
            version,
            release_date,
            latest_stable,
            latest_beta,
            latest_nightly
        FROM
            rust_versions
        WHERE
            version = ?1",
    )?;

    let mut version_info = get_version_info_rows(stmt, &query_version)?;

    if let Some(version) = version_info.first_mut() {
        version.components = get_rust_components(conn, &version.version)?;
    }

    Ok(version_info)
}

fn get_version_info_rows(mut statement: Statement, version: &String) -> RustVersionsAggResult {
    statement
        .query_map([version], |row| {
            Ok(RustVersion {
                version: row.get(0)?,
                release_date: row.get(1)?,
                latest_stable: row.get(2)?,
                latest_beta: row.get(3)?,
                latest_nightly: row.get(4)?,
                components: vec![],
                profiles: None,
                renames: None,
                artefacts: None,
            })
        })
        .and_then(Iterator::collect)
}
