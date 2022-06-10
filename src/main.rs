#![feature(test)]

mod cache;
mod matcher;

fn main() {
    let contents = std::fs::read_to_string("config.json").expect("failed to open config");
    let linker = matcher::ContextLinker::new(&contents).expect("failed to create ContextLinker");
    
}
