#![feature(file_lock)]
mod batch;
mod data;
pub mod db;
pub mod error;
mod fio;
mod index;
pub mod iterator;
mod merge;
pub mod options;
mod util;

pub fn add(left: usize, right: usize) -> usize {
    left + right
}

#[cfg(test)]
mod db_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
