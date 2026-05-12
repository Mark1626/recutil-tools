use std::env;
use std::path::PathBuf;
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=wrapper.h");
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=DOCS_RS");
    println!("cargo:rerun-if-env-changed=RECUTILS_INCLUDE_DIR");
    println!("cargo:rerun-if-env-changed=RECUTILS_LIB_DIR");
    println!("cargo:rerun-if-env-changed=RECUTILS_PREFIX");

    // docs.rs runs in a Debian sandbox without librec or its headers. Skip
    // bindgen + linking and ship the checked-in pre-generated bindings so
    // rustdoc has the item signatures it needs. The bindings are
    // architecture-shaped but rustdoc only renders names + types, not ABI.
    if env::var_os("DOCS_RS").is_some() {
        let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
        std::fs::copy("src/bindings.docsrs.rs", out_path.join("bindings.rs"))
            .expect("failed to stage pre-generated bindings for docs.rs build");
        return;
    }

    let (include_dir, lib_dir) = locate_recutils();

    if let Some(dir) = &lib_dir {
        println!("cargo:rustc-link-search=native={}", dir.display());
    }
    println!("cargo:rustc-link-lib=dylib=rec");

    let mut builder = bindgen::Builder::default()
        .header("wrapper.h")
        .allowlist_function("rec_.*")
        .allowlist_type("rec_.*")
        .allowlist_var("REC_.*")
        .allowlist_var("MSET_.*")
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()));

    if let Some(dir) = &include_dir {
        builder = builder.clang_arg(format!("-I{}", dir.display()));
    }

    let bindings = builder
        .generate()
        .expect("failed to generate librec bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("failed to write bindings.rs");
}

fn locate_recutils() -> (Option<PathBuf>, Option<PathBuf>) {
    if let Ok(prefix) = env::var("RECUTILS_PREFIX") {
        let p = PathBuf::from(prefix);
        return (Some(p.join("include")), Some(p.join("lib")));
    }

    let include = env::var("RECUTILS_INCLUDE_DIR").ok().map(PathBuf::from);
    let lib = env::var("RECUTILS_LIB_DIR").ok().map(PathBuf::from);
    if include.is_some() || lib.is_some() {
        return (include, lib);
    }

    if let Some(prefix) = brew_prefix("recutils") {
        return (Some(prefix.join("include")), Some(prefix.join("lib")));
    }

    (None, None)
}

fn brew_prefix(formula: &str) -> Option<PathBuf> {
    let out = Command::new("brew").args(["--prefix", formula]).output().ok()?;
    if !out.status.success() {
        return None;
    }
    let path = String::from_utf8(out.stdout).ok()?;
    let trimmed = path.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(PathBuf::from(trimmed))
    }
}
