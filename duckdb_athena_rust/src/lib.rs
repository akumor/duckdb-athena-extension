// Copyright 2023 Lance Developers
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod connection;
mod data_chunk;
mod database;
mod error;
mod function_info;
mod logical_type;
pub mod table_function;
mod value;
mod vector;

pub use connection::Connection;
pub use data_chunk::DataChunk;
pub use database::Database;
pub use error::{Error, Result};
pub use function_info::FunctionInfo;
pub use logical_type::{LogicalType, LogicalTypeId};
pub use value::Value;
pub use vector::{FlatVector, Inserter, ListVector, StructVector, Vector};
use std::mem::size_of;

pub use libduckdb_sys::{duckdb_vector_size, duckdb_bind_info, duckdb_data_chunk, duckdb_free, duckdb_function_info, duckdb_init_info, _duckdb_database, duckdb_library_version};

#[allow(clippy::all)]
pub mod ffi {
    #![allow(non_upper_case_globals)]
    #![allow(non_camel_case_types)]
    #![allow(non_snake_case)]
    #![allow(unused)]
    #![allow(improper_ctypes)]
    #![allow(clippy::upper_case_acronyms)]
}

/// # Safety
/// This function is obviously unsafe
pub unsafe fn malloc_struct<T>() -> *mut T {
    libduckdb_sys::duckdb_malloc(size_of::<T>()).cast::<T>()
}
