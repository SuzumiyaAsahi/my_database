use std::result;

pub enum Errors {}

pub type Result<T> = result::Result<T, Errors>;
