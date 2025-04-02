
use actix_files::Files;
use actix_web::{App, HttpResponse, HttpServer, Responder, get, middleware, web, web::Data};
use env_logger::Env;
use oasgen::{Server, oasgen};
use tera::{Context, Tera};

mod db;
use db::{ComponentQueries, Pool, VersionQueries};

pub mod models;
use models::{Component, RustVersion};

// --- HTML Rendering Handlers ---

#[get("/")]
pub async fn hello(tera: Data<Tera>, db: web::Data<Pool>) -> impl Responder {
    let mut ctx = Context::new();
    let versions = db::execute_versions(&db, VersionQueries::GetAllVersions, None)
        .await
        .unwrap();
    ctx.insert("versions", &versions);
    let named_channels = db::execute_versions(&db, VersionQueries::GetNamedChannels, None)
        .await
        .unwrap();
    ctx.insert("named_channels", &named_channels);
    HttpResponse::Ok().body(tera.render("index.tera", &ctx).unwrap())
}

#[get("/info/{version}")]
pub async fn versioninfo(
    tera: Data<Tera>,
    path: web::Path<String>,
    db: web::Data<Pool>,
) -> impl Responder {
    let mut ctx = Context::new();
    let rustversion =
        db::execute_versions(&db, VersionQueries::GetVersionInfo, Some(path.to_string()))
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
    ctx.insert("version", &rustversion);
    HttpResponse::Ok().body(tera.render("versioninfo.tera", &ctx).unwrap())
}

#[get("/info/all")]
pub async fn allversions(tera: Data<Tera>, db: web::Data<Pool>) -> impl Responder {
    let mut ctx = Context::new();
    let versions = db::execute_versions(&db, VersionQueries::GetAllVersions, None)
        .await
        .unwrap();
    ctx.insert("versions", &versions);
    HttpResponse::Ok().body(tera.render("allversions.tera", &ctx).unwrap())
}

#[get("/info/component/{name}/{version}")]
pub async fn component(
    tera: Data<Tera>,
    path: web::Path<(String, String)>,
    db: web::Data<Pool>,
) -> impl Responder {
    let mut ctx = Context::new();
    let component = db::execute_components(
        &db,
        ComponentQueries::GetRustComponent,
        path.0.to_string(),
        path.1.to_string(),
    )
    .await
    .unwrap();
    ctx.insert("rustversion", &path.1.to_string());
    ctx.insert("component", &component);
    HttpResponse::Ok().body(tera.render("component.tera", &ctx).unwrap())
}

// --- API Handlers ---

#[oasgen]
pub async fn versioninfoapi(
    path: web::Path<String>,
    db: web::Data<Pool>,
) -> Result<web::Json<RustVersion>, Box<dyn std::error::Error>> {
    let version_str = path.into_inner();
    let rustversion = db::execute_versions(&db, VersionQueries::GetVersionInfo, Some(version_str))
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| format!("Version not found"))?;
    Ok(web::Json(rustversion))
}

#[oasgen]
pub async fn componentinfoapi(
    path: web::Path<(String, String)>,
    db: web::Data<Pool>,
) -> Result<web::Json<Vec<Component>>, Box<dyn std::error::Error>> {
    let (name, version) = path.into_inner();
    let rust_component =
        db::execute_components(&db, ComponentQueries::GetRustComponent, name, version).await?;
    Ok(web::Json(vec![rust_component]))
}

#[oasgen]
pub async fn namedchannelsapi(
    db: web::Data<Pool>,
) -> Result<web::Json<Vec<RustVersion>>, Box<dyn std::error::Error>> {
    let named_channels = db::execute_versions(&db, VersionQueries::GetNamedChannels, None).await?;
    Ok(web::Json(named_channels))
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    let tera = Data::new(Tera::new("./templates/*").unwrap());

    let manager = r2d2_sqlite::SqliteConnectionManager::file("rust_versions.sqlite3");
    let pool = r2d2::Pool::new(manager).unwrap();
    let pool_data = web::Data::new(pool.clone());

    let oasgen_server = Server::actix() // Use default Server builder
        .get("/api/v1/version/{version}", versioninfoapi)
        .get("/api/v1/component/{name}/{version}", componentinfoapi)
        .get("/api/v1/named_channels", namedchannelsapi)
        .route_json_spec("/openapi.json")
        .swagger_ui("/swagger-ui/") // Must have a trailing slash
        .freeze();

    HttpServer::new(move || {
        App::new()
            .app_data(pool_data.clone())
            .app_data(tera.clone())
            .wrap(middleware::Logger::new("%a %{User-Agent}i"))
            // Mount non-API routes
            .service(hello)
            .service(versioninfo)
            .service(allversions)
            .service(component)
            .service(Files::new("/static", "./static"))
            // Mount oasgen managed services
            .service(oasgen_server.clone().into_service())
    })
    .bind(("0.0.0.0", 8080))?
    .run()
    .await
}
