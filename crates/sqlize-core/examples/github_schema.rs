use std::path::Path;

use sqlize_core::catalog::ddl::catalog_ddl;
use sqlize_core::spec::load_catalog;

fn main() {
    let spec_path = Path::new("specs/github.json");

    // Only load a subset of tags to keep output manageable
    let tags = ["repos", "issues", "pulls", "users"];
    let (catalog, _info) = load_catalog(spec_path, Some(&tags)).expect("failed to load catalog");

    println!("=== SQLize: GitHub API Schema ===\n");
    println!("Tables generated: {}\n", catalog.table_count());

    // Print table names
    for table in catalog.tables() {
        let required: Vec<_> = table.required_params().map(|c| c.name.as_str()).collect();
        let response_cols = table.result_columns().count();
        let pushdown_cols = table.pushdown_params().count();

        println!(
            "  {:<30} {:>3} columns ({} response, {} filterable) | required: [{}]",
            table.name,
            table.columns.len(),
            response_cols,
            pushdown_cols,
            required.join(", "),
        );
    }

    println!("\n=== DDL ===\n");
    println!("{}", catalog_ddl(&catalog));
}
