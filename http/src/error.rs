use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};

pub enum MyError {
    FailToGetKeyInEngine,
    FailToListKeys,
    FailToPutKey,
    FailToDelte,
    FailToGetState,
}

impl IntoResponse for MyError {
    fn into_response(self) -> Response {
        let body = match self {
            MyError::FailToGetKeyInEngine => "fail to get key in engine",
            MyError::FailToListKeys => "fail to list keys",
            MyError::FailToPutKey => "fail to put key",
            MyError::FailToDelte => "fail to delete",
            MyError::FailToGetState => "fail to get state",
        };

        // it's often easiest to implement `IntoResponse` by calling other implementations
        (StatusCode::INTERNAL_SERVER_ERROR, body).into_response()
    }
}
