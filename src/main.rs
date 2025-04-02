use actix_files::Files;
use actix_web::{
    App,
    HttpRequest,
    HttpResponse,
    HttpServer,
    Responder,
    Result,
    dev::ServiceResponse,
    error, // Added error module
    get,
    http::StatusCode,
    middleware,
    middleware::{ErrorHandlerResponse, ErrorHandlers},
    web::{self, Data},
};
use env_logger::Env;
use oasgen::{Server, oasgen};
use tera::{Context, Tera};

mod db;
use db::{ComponentQueries, Pool, VersionQueries};

pub mod models;
use models::{Component, RustVersion};

// --- Error Pages ---

async fn not_found(
    req: HttpRequest,
    tera: web::Data<Tera>,
) -> Result<HttpResponse, actix_web::Error> {
    let mut ctx = Context::new();
    let status_code = StatusCode::NOT_FOUND;

    let path = req.path();
    ctx.insert("path", path);
    ctx.insert("status_code", &status_code.as_u16());
    ctx.insert(
        "reason",
        status_code.canonical_reason().unwrap_or("Not Found"),
    );

    let body = tera
        .render("error.tera", &ctx)
        .map_err(|e| error::ErrorInternalServerError(format!("Template error: {}", e)))?;

    Ok(HttpResponse::build(status_code)
        .content_type("text/html")
        .body(body))
}

fn render_error_page<B>(res: ServiceResponse<B>) -> Result<ErrorHandlerResponse<B>> {
    // Try to get Tera engine from app data
    let tera = res.request().app_data::<web::Data<Tera>>().cloned();

    // Extract original status code before consuming response
    let status_code = res.status();

    let (req, _res) = res.into_parts(); // Keep request, discard original response

    let body = match tera {
        Some(tera_data) => {
            let mut ctx = Context::new();
            // Pass status code and reason to the generic template
            ctx.insert("status_code", &status_code.as_u16());
            ctx.insert("reason", status_code.canonical_reason().unwrap_or("Error"));
            if std::env::var("DEVELOPMENT_MODE").unwrap_or_else(|_| "false".to_string()) == "true" {
                if let Some(err) = _res.error() {
                    ctx.insert("error_message", &format!("{}", err));
                }
            }

            tera_data
                .render("error.tera", &ctx) // Use the generic template
                .unwrap_or_else(|e| format!("Error rendering template 'error.tera': {}", e))
        }
        None => format!(
            "Internal Server Error: Template engine not available (Status: {})",
            status_code
        ),
    };

    // Create a new response with the rendered body and the original status code
    let new_res = HttpResponse::build(status_code) // Keep original status code
        .content_type("text/html")
        .body(body);

    Ok(ErrorHandlerResponse::Response(ServiceResponse::new(
        req,
        new_res.map_into_right_body(),
    )))
}

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
    // %{r}a unwraps the remote address from the request; use behind a reverse proxy else it's trivial to spoof
    let log_format = "%{r}a %U %D %b %s";
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
        let error_handlers = ErrorHandlers::new()
            .handler(StatusCode::BAD_REQUEST, render_error_page)           // 400
            .handler(StatusCode::INTERNAL_SERVER_ERROR, render_error_page) // 500
            .handler(StatusCode::METHOD_NOT_ALLOWED, render_error_page)   // 405
            .handler(StatusCode::NOT_IMPLEMENTED, render_error_page)        // 501
            .handler(StatusCode::SERVICE_UNAVAILABLE, render_error_page)   // 503
        ;
        App::new()
            .app_data(pool_data.clone())
            .app_data(tera.clone())
            .wrap(middleware::Logger::new(log_format))
            .wrap(error_handlers)
            // Mount non-API routes
            .service(hello)
            .service(versioninfo)
            .service(allversions)
            .service(component)
            .service(Files::new("/static", "./static"))
            // Mount oasgen managed services
            .service(oasgen_server.clone().into_service())
            .default_service(web::route().to(not_found)) // Catch-all for non-API routes; display 404
    })
    .bind(("0.0.0.0", 8080))?
    .run()
    .await
}
