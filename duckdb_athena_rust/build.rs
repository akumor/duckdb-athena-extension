use build_script::cargo_rerun_if_changed;
use std::path::PathBuf;
use std::{env, path::Path};

fn main() {
    let duckdb_root = Path::new(&env::var("CARGO_MANIFEST_DIR").unwrap())
        .join("duckdb")
        .canonicalize()
        .expect("duckdb source root");

    let header = "src/duckdb_athena_rust.h";
    cargo_rerun_if_changed(header);

    let duckdb_include = duckdb_root.join("src/include");
    let bindings = bindgen::Builder::default()
        .header(header)
        .clang_arg("-xc++")
        .clang_arg("-I")
        .clang_arg(duckdb_include.to_string_lossy())
        .derive_debug(true)
        .derive_default(true)
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        .generate()
        .expect("Unable to generate bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");

    cc::Build::new()
    .include(duckdb_include)
    .include("duckdb/src/include") // Relative import due to https://github.com/rust-lang/cc-rs/issues/169
    .flag_if_supported("-Wno-unused-parameter")
    .flag_if_supported("-Wno-redundant-move")
    .flag_if_supported("-std=c++17")
    .cpp(true)
    .file("src/duckdb_athena_rust.cc")
    .compile("duckdb_athena_rust");   
}
