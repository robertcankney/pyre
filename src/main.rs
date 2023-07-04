use std::io::{Error, ErrorKind};
use actix_web::{
    web::{self, Data},
    App, HttpServer,
};
use tracing;
use tracing_subscriber;

mod cache;
mod rest;
mod config;

#[actix_web::main]
async fn main() -> Result<(), Box<Error>> {
    let format = tracing_subscriber::fmt::format().json();
    let subscriber = tracing_subscriber::fmt()
        // .with_max_level(tracing_subscriber::filter::LevelFilter::DEBUG)
        .event_format(format)
        .with_writer(std::io::stdout)
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .map_err(|err| eprintln!("Unable to set global default subscriber: {}", err))
        .unwrap();

    let mut args = std::env::args().collect::<Vec<String>>();
    let cfg = args.pop().ok_or(config::ConfigError{msg: "missing a config string".to_string()}).map_err(to_io_err)?;
    let linker = cfg.try_into().map_err(to_io_err)?;

    let handler = rest::Handler::new(linker);
    let wrapper = Data::new(handler);

    HttpServer::new(move || {
        App::new()
            .wrap(tracing_actix_web::TracingLogger::default())
            .app_data(wrapper.clone())
            .route(
                "rate/{collection}/{key}",
                web::get().to(rest::Handler::handle),
            )
    })
    .bind(("0.0.0.0", 8080))?
    .run()
    .await
    .map_err(|e| Box::new(e))
}

fn to_io_err<E: Into<Box<dyn std::error::Error + Send + Sync>>>(err: E) -> Box<std::io::Error> {
    Box::new(std::io::Error::new(ErrorKind::Other, err))
}