pub mod daemon;
pub mod db;
pub mod utils;
const VERSION: &str = env!("CARGO_PKG_VERSION");
const SERVICE: &str = "gazpacho";

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
