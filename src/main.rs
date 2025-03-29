use actix_files::Files;
use actix_web::{App, HttpResponse, HttpServer, Responder, get, middleware, web, web::Data};
use env_logger::Env;
use tera::{Context, Tera};

mod db;
use db::{Pool, Queries};

pub mod models;

#[get("/")]
pub async fn hello(tera: Data<Tera>, db: web::Data<Pool>) -> impl Responder {
    let mut ctx = Context::new();

    let versions = db::execute(&db, Queries::GetAllVersions, None)
        .await
        .unwrap();
    ctx.insert("versions", &versions);
    let named_channels = db::execute(&db, Queries::GetNamedChannels, None)
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

    let rustversion = db::execute(&db, Queries::GetVersionInfo, Some(path.to_string()))
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

    let versions = db::execute(&db, Queries::GetAllVersions, None)
        .await
        .unwrap();
    ctx.insert("versions", &versions);
    HttpResponse::Ok().body(tera.render("allversions.tera", &ctx).unwrap())
}

#[get("api/v1/version/{version}")]
pub async fn versioninfoapi(path: web::Path<String>, db: web::Data<Pool>) -> impl Responder {
    let rustversion = db::execute(&db, Queries::GetVersionInfo, Some(path.to_string()))
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    HttpResponse::Ok().json(rustversion)
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("debug")).init();
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
            .service(Files::new("/static", "./static")) // No need to enable listing
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}
