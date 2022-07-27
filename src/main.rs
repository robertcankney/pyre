#![feature(test)]

use actix_web::{
    web::{self, Data},
    App, HttpServer,
};
use tracing;
use tracing_subscriber;

mod cache;
mod matcher;
mod rest;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let format = tracing_subscriber::fmt::format().json();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing_subscriber::filter::LevelFilter::INFO)
        .event_format(format)
        .with_writer(std::io::stdout)
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .map_err(|err| eprintln!("Unable to set global default subscriber: {}", err))
        .unwrap();

    let contents = std::fs::read_to_string("config.json").expect("failed to open config");
    let linker = matcher::ContextLinker::new(&contents).expect("failed to create ContextLinker");
    let handler = rest::Handler::new(linker);
    let wrapper = Data::new(handler);

    HttpServer::new(move || {
        App::new().app_data(wrapper.clone()).route(
            "rate/{collection}/{key}",
            web::get().to(rest::Handler::handle),
        )
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}
