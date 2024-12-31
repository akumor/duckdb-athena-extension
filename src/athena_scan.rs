/*use aws_sdk_athena::{
    model::{
        QueryExecutionState::{self, *},
        ResultConfiguration, ResultSet,
    },
    output::{GetQueryExecutionOutput, GetQueryResultsOutput},
    paginator::GetQueryResultsPaginator,
    
};*/
use crate::types::{map_type, populate_column};
use anyhow::{anyhow, Result};
use aws_sdk_athena::{
    error::GetQueryResultsError,
    model::{
        QueryExecutionState,
        ResultSet,
        ResultSetMetadata,
        Row,
    },
    output::{GetQueryExecutionOutput, GetQueryResultsOutput},
    paginator::GetQueryResultsPaginator,
    types::SdkError,
    Client as AthenaClient,
};
use aws_sdk_glue::model::Column;
use duckdb::core::{
    DataChunkHandle,
    LogicalTypeHandle,
    LogicalTypeId,
};
use futures::Stream;
use libduckdb_sys as ffi;
use std::{
    pin::Pin,
    task::{Context, Poll},
};


pub struct ResultStream {
    pub stream:
        Pin<Box<dyn Stream<Item = Result<GetQueryResultsOutput, SdkError<GetQueryResultsError>>>>>,
}

impl ResultStream {
    pub fn new(
        stream: Box<
            dyn Stream<Item = Result<GetQueryResultsOutput, SdkError<GetQueryResultsError>>>,
        >,
    ) -> Self {
        Self {
            stream: stream.into(),
        }
    }
}

impl Stream for ResultStream {
    type Item = Result<GetQueryResultsOutput, SdkError<GetQueryResultsError>>;

    // https://stackoverflow.com/questions/72926989/how-to-implement-trait-futuresstreamstream was ridiculously helpful
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.get_mut().stream.as_mut().poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(v) => {
                // Do what you need to do with v here.
                Poll::Ready(v)
            }
        }
    }
}

pub fn athena_query_status(resp: &GetQueryExecutionOutput) -> Option<&QueryExecutionState> {
    resp.query_execution().unwrap().status().unwrap().state()
}

pub fn total_execution_time(resp: &GetQueryExecutionOutput) -> Option<i64> {
    resp.query_execution()
        .unwrap()
        .statistics()
        .unwrap()
        .total_execution_time_in_millis()
}

pub async fn get_query_result(client: &AthenaClient, query_execution_id: String) -> Result<ResultSet> {
    let resp = anyhow::Context::with_context(
        client
            .get_query_results()
            .set_query_execution_id(Some(query_execution_id.clone()))
            .send()
            .await,
        || {
            format!(
                "could not get query results for query id {}",
                query_execution_id
            )
        },
    )?;

    Ok(resp
        .result_set()
        .ok_or_else(|| anyhow!("could not get query result"))?
        .clone())
}

pub async fn get_query_result_paginator(
    client: &AthenaClient,
    query_execution_id: String,
) -> GetQueryResultsPaginator {
    client
        .get_query_results()
        .set_query_execution_id(Some(query_execution_id.clone()))
        .into_paginator()
}

pub unsafe extern "C" fn duckdb_free_wrapper(ptr: *mut std::ffi::c_void) {
    println!("DEBUG | duckdb_free_wrapper | Freeing memory at {:?}", ptr);
    ffi::duckdb_free(ptr); // Calls the original function
}

// Convert AWS Glue table column type to duckdb logical type
pub fn to_duckdb_logical_type(column: &Column) -> Result<LogicalTypeHandle, Box<dyn std::error::Error>> {
    let type_id = match column.r#type().unwrap_or("varchar").to_string().as_str() {
        "boolean" => LogicalTypeHandle::from(LogicalTypeId::Boolean),
        "tinyint" => LogicalTypeHandle::from(LogicalTypeId::Tinyint),
        "smallint" => LogicalTypeHandle::from(LogicalTypeId::Smallint),
        "int" => LogicalTypeHandle::from(LogicalTypeId::Integer),
        "integer" => LogicalTypeHandle::from(LogicalTypeId::Integer),
        "bigint" => LogicalTypeHandle::from(LogicalTypeId::Bigint),
        "float" => LogicalTypeHandle::from(LogicalTypeId::Float),
        "real" => LogicalTypeHandle::from(LogicalTypeId::Float),
        "double" => LogicalTypeHandle::from(LogicalTypeId::Double),
        "string" => LogicalTypeHandle::from(LogicalTypeId::Varchar),
        "timestamp" => LogicalTypeHandle::from(LogicalTypeId::TimestampMs),
        "array" => LogicalTypeHandle::from(LogicalTypeId::List),
        "map" => LogicalTypeHandle::from(LogicalTypeId::Map),
        "struct" => LogicalTypeHandle::from(LogicalTypeId::Struct),
        "varchar" => LogicalTypeHandle::from(LogicalTypeId::Varchar),
        _ => {
            return Err(format!("Unsupported data type: {}, please file an issue https://github.com/dacort/duckdb-athena-extension", column.r#type().unwrap_or("varchar").to_string()).into());
        }
    };
    Ok(type_id)
}

pub fn result_set_to_duckdb_data_chunk(
    rows: &[Row],
    metadata: &ResultSetMetadata,
    chunk: &DataChunkHandle,
) -> Result<()> {
    // Fill the row
    // This is asserting the wrong thing (row length vs. column length)
    // assert!_eq!(rs.rows().unwrap().len(), chunk.num_columns());
    // let rows = &rs.rows().unwrap()[1..];
    let result_size = rows.len();

    for row_idx in 0..result_size {
        let row = &rows[row_idx];
        let row_data = row.data().unwrap();
        for col_idx in 0..row_data.len() {
            let value = row_data[col_idx].var_char_value().unwrap();
            let colinfo = &metadata.column_info().unwrap()[col_idx];
            let ddb_type = map_type(colinfo.r#type().unwrap().to_string()).unwrap();
            unsafe { populate_column(value, ddb_type, chunk, row_idx, col_idx) };
        }
    }

    chunk.set_len(result_size);

    Ok(())
}
