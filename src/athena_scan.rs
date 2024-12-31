/*use aws_sdk_athena::{
    model::{
        QueryExecutionState::{self, *},
        ResultConfiguration, ResultSet,
    },
    output::{GetQueryExecutionOutput, GetQueryResultsOutput},
    paginator::GetQueryResultsPaginator,
    
};*/
use anyhow::{anyhow, Result};
use aws_sdk_athena::{
    error::GetQueryResultsError,
    model::{
        QueryExecutionState,
        ResultSet,
    },
    output::{GetQueryExecutionOutput, GetQueryResultsOutput},
    paginator::GetQueryResultsPaginator,
    types::SdkError,
    Client as AthenaClient,
};
use futures::Stream;
use libduckdb_sys as ffi;
use std::{
    pin::Pin,
    task::{Context, Poll},
};


pub struct ResultStream {
    stream:
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
    ffi::duckdb_free(ptr); // Calls the original function
}
