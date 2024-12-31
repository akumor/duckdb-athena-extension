#![allow(dead_code)]
extern crate duckdb;
extern crate duckdb_loadable_macros;
use athena_scan::{
    ResultStream,
    athena_query_status,
    duckdb_free_wrapper,
    get_query_result,
    get_query_result_paginator,
    total_execution_time,
};
use aws_sdk_athena::{
    model::{
        QueryExecutionState::{self, *},
        ResultConfiguration,
    },
    Client as AthenaClient
};
use aws_sdk_glue::Client as GlueClient;
use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, Free, FunctionInfo, InitInfo, VTab},
    Connection, Result,
};
use duckdb_loadable_macros::duckdb_entrypoint_c_api;
use futures::executor::block_on;
use libduckdb_sys as ffi;
use std::{
    ffi::{c_char, CStr, CString},
    thread,
};
use tokio::{runtime::Runtime, time::Duration};
pub mod error;
mod athena_scan;
mod table_function;
mod types;

lazy_static::lazy_static! {
    static ref RUNTIME: Runtime = tokio::runtime::Runtime::new()
            .expect("Creating Tokio runtime");
}

#[repr(C)]
struct AthenaScanBindData {
    tablename: *mut c_char,
    output_location: *mut c_char,
    maxrows: i32,

}

const DEFAULT_MAXROWS: i32 = 10000;

impl AthenaScanBindData {
    fn new(tablename: &str, output_location: &str) -> Self {
        Self {
            tablename: CString::new(tablename).expect("Table name").into_raw(),
            output_location: CString::new(output_location)
                .expect("S3 output location")
                .into_raw(),
            maxrows: DEFAULT_MAXROWS as i32,
        }
    }
}

impl Free for AthenaScanBindData {
    fn free(&mut self) {
        unsafe {
            if self.tablename.is_null() {
                return;
            }
            drop(CString::from_raw(self.tablename));
            if self.output_location.is_null() {
                return;
            }
            drop(CString::from_raw(self.output_location));
        }
        let _ = self.maxrows;
    }
}

#[repr(C)]
struct AthenaScanInitData {
    stream: *mut ResultStream,
    pagination_index: u32,
    done: bool,
}

impl AthenaScanInitData {
    fn new(stream: Box<ResultStream>) -> Self {
        Self {
            stream: Box::into_raw(stream),
            done: false,
            pagination_index: 0,
        }
    }
}

impl Free for AthenaScanInitData {}

struct AthenaScanVTab;

impl VTab for AthenaScanVTab {
    type InitData = AthenaScanInitData;
    type BindData = AthenaScanBindData;

    unsafe fn init(info: &InitInfo, data: *mut AthenaScanInitData) -> Result<(), Box<dyn std::error::Error>> {
        // reimplemented from read_athena_init
        let bind_info = info.get_bind_data::<AthenaScanBindData>();

        // Extract the table name and output location from the function parameters
        let tablename = CStr::from_ptr((*bind_info).tablename).to_str().unwrap();
        let output_location = CStr::from_ptr((*bind_info).output_location)
            .to_str()
            .unwrap();
        let maxrows = (*bind_info).maxrows;

        let config = block_on(aws_config::load_from_env());
        let client = AthenaClient::new(&config);
        let result_config = ResultConfiguration::builder()
            .set_output_location(Some(output_location.to_owned()))
            .build();

        let mut query = format!("SELECT * FROM {}", tablename);
        if maxrows >= 0 {
            query = format!("{} LIMIT {}", query, maxrows);
        }

        let athena_query = client
            .start_query_execution()
            .set_query_string(Some(query))
            .set_result_configuration(Some(result_config))
            .set_work_group(Some("primary".to_string()))
            .send();

        // TODO: Use unwrap_or maybe? Docs recommend not to use this because it can panic.
        let resp = crate::RUNTIME
            .block_on(athena_query)
            .expect("could not start query");

        let query_execution_id = resp.query_execution_id().unwrap_or_default();
        println!(
            "Running Athena query, execution id: {}",
            &query_execution_id
        );

        let mut state: QueryExecutionState;

        loop {
            let get_query = client
                .get_query_execution()
                .set_query_execution_id(Some(query_execution_id.to_string()))
                .send();
            
            let resp = crate::RUNTIME
                .block_on(get_query)
                .expect("Could not get query status");
            state = athena_query_status(&resp).expect("could not get query status").clone();

            match state {
                Queued | Running => {
                    thread::sleep(Duration::from_secs(5));
                    println!("State: {:?}, sleep 5 secs ...", state);
                }
                Cancelled | Failed => {
                    println!("State: {:?}", state);
    
                    match crate::RUNTIME
                        .block_on(get_query_result(&client, query_execution_id.to_string()))
                    {
                        Ok(result) => println!("Result: {:?}", result),
                        Err(e) => println!("Result error: {:?}", e),
                    }
    
                    break;
                }
                _ => {
                    let millis = total_execution_time(&resp).unwrap();
                    println!("Total execution time: {} millis", millis);
    
                    // let stream = match crate::RUNTIME.block_on(async {
                    //     let paginator = get_query_result_paginator(&client, query_execution_id.to_string()).await;
                    //     let results = paginator.send();
                    //     results
                    // }) {
                    //     Ok(s) => Box::new(s),
                    // };
                    let paginator = crate::RUNTIME.block_on(async {
                        get_query_result_paginator(&client, query_execution_id.to_string()).await
                    });
                    let stream = paginator.send();
    
                    let init_data = Box::new(AthenaScanInitData::new(Box::new(ResultStream::new(Box::new(
                        stream,
                    )))));
                    info.set_init_data(Box::into_raw(init_data).cast(), Some(duckdb_free_wrapper));
                    break;
                }
            }
        }

        // read_athena_init
        unsafe {
            (*data).done = false;
        }
        Ok(())    
    }

    unsafe fn bind(bind: &BindInfo, data: *mut AthenaScanBindData) -> Result<(), Box<dyn std::error::Error>> {
        // TODO: reimplement read_athena_bind

        bind.add_result_column("value", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        let tablename_param = bind.get_parameter(0).to_string();
        let output_location_param = bind.get_parameter(1).to_string();
        let maxrows_param: i32;
        match bind.get_named_parameter("maxrows") {
            Some(maxrows) => {
                maxrows_param = maxrows.to_int64() as i32;
            }
            None => {
                maxrows_param = DEFAULT_MAXROWS;
            }
        }

        // We need to go to the Glue Data Catalog and fetch the column tables for that table.
        // For now, we only support the `default` table.
        let config = block_on(aws_config::load_from_env());
        let client = GlueClient::new(&config);

        let table = client
            .get_table()
            .database_name("default")
            .name(tablename_param.to_string())
            .send();

        match crate::RUNTIME.block_on(table) {
            Ok(resp) => {
                let columns = resp
                    .table()
                    .unwrap()
                    .storage_descriptor()
                    .unwrap()
                    .columns();
                for column in columns.unwrap() {
                    let typ = LogicalType::new(
                        map_type(column.r#type().unwrap_or("varchar").to_string())
                            .expect("Could not get type"),
                    );
                    bind.add_result_column(column.name().unwrap(), typ);
                }
            }
            Err(err) => {
                bind.set_error(&err.into_service_error().to_string());
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    &*err.into_service_error().to_string(),
                )));
            }
        }

        unsafe {
            (*data).tablename = CString::new(tablename_param).unwrap().into_raw();
            (*data).output_location = CString::new(output_location_param).unwrap().into_raw();
            (*data).maxrows = maxrows_param;
        }
        Ok(())
    }

    unsafe fn func(func: &FunctionInfo, output: &mut DataChunkHandle) -> Result<(), Box<dyn std::error::Error>> {
        // TODO: reimplement read_athena
        let init_info = func.get_init_data::<AthenaScanInitData>();
        let bind_info = func.get_bind_data::<AthenaScanBindData>();

        unsafe {
            if (*init_info).done {
                output.set_len(0);
            } else {
                (*init_info).done = true;
                let vector = output.flat_vector(0);
                let tablename = CString::from_raw((*bind_info).tablename);
                let output_location = CString::from_raw((*bind_info).output_location);
                let maxrows = (*bind_info).maxrows;
                // TODO: fix to actually perform the Athena query
                let result = CString::new(format!("Athena! tablename = {} | output_location = {} | maxrows = {} | 🐥", tablename.to_str()?, output_location.to_str()?, maxrows))?;
                // Can't consume the CString
                (*bind_info).tablename = CString::into_raw(tablename);
                (*bind_info).output_location = CString::into_raw(output_location);
                vector.insert(0, result);
                output.set_len(1);
            }
        }
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![
            LogicalTypeHandle::from(LogicalTypeId::Varchar), // tablename
            LogicalTypeHandle::from(LogicalTypeId::Varchar), // output_location
        ])
    }

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        Some(vec![("maxrows".to_string(), LogicalTypeHandle::from(LogicalTypeId::Integer))]) // maxrows
    }
}

const EXTENSION_NAME: &str = env!("CARGO_PKG_NAME");

#[duckdb_entrypoint_c_api(ext_name = "athena", min_duckdb_version = "v0.0.1")]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<()> {
    con.register_table_function::<AthenaScanVTab>("athena_scan")
        .expect("Failed to register athena table function");
    Ok(())
}
