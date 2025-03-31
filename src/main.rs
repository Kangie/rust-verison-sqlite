use actix_files::Files;
use actix_web::{App, HttpResponse, HttpServer, Responder, get, middleware, web, web::Data};
use env_logger::Env;
use tera::{Context, Tera};

mod db;
use db::{ComponentQueries, Pool, VersionQueries};

pub mod models;

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

#[get("api/v1/version/{version}")]
pub async fn versioninfoapi(path: web::Path<String>, db: web::Data<Pool>) -> impl Responder {
    let rustversion =
        db::execute_versions(&db, VersionQueries::GetVersionInfo, Some(path.to_string()))
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
    HttpResponse::Ok().json(rustversion)
}

#[get("/api/v1/component/{name}/{version}")]
pub async fn componentinfoapi(
    path: web::Path<(String, String)>,
    db: web::Data<Pool>,
) -> impl Responder {
    let rust_component = db::execute_components(
        &db,
        ComponentQueries::GetRustComponent,
        path.0.to_string(),
        path.1.to_string(),
    )
    .await
    .unwrap();

    HttpResponse::Ok().json(rust_component)
}

#[get("/api/v1/named_channels")]
pub async fn namedchannelsapi(db: web::Data<Pool>) -> impl Responder {
    let named_channels = db::execute_versions(&db, VersionQueries::GetNamedChannels, None)
        .await
        .unwrap();
    HttpResponse::Ok().json(named_channels)
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    let tera = Data::new(Tera::new("./templates/*").unwrap());

    let manager = r2d2_sqlite::SqliteConnectionManager::file("rust_versions.sqlite3");
    let pool = r2d2::Pool::new(manager).unwrap();

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(pool.clone()))
            .wrap(middleware::Logger::new("%a %{User-Agent}i"))
            .app_data(tera.clone())
            .service(hello)
            .service(versioninfo)
            .service(allversions)
            .service(versioninfoapi)
            .service(component)
            .service(componentinfoapi)
            .service(namedchannelsapi)
            .service(Files::new("/static", "./static")) // No need to enable listing
    })
    .bind(("0.0.0.0", 8080))?
    .run()
    .await
}
