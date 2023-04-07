use polars::prelude::*;
use std::fs;
use std::error::Error;
use csv::{ReaderBuilder, Writer};

fn read_csv_files() -> Result<(), Box<dyn Error>>  {
    // Read all files in the current directory with .csv extension
    let files = fs::read_dir("./data/")?
        .filter_map(|entry| {
            let path = entry.unwrap().path();
            if path.is_file() && path.extension().unwrap_or_default() == "csv" {
                Some(path)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    println!("Files name .csv => {:?}", files);

    //iter files
    for file in files {
        let mut rdr = ReaderBuilder::new()
            .has_headers(true)
            .from_path(file)?;
        println!("reader => {:?}", rdr);

        for result in rdr.records() {
            let record = result?;
            println!("record => {:?}", record);
        }
    }
    Ok(())
}


fn read_csv() -> PolarsResult<DataFrame> {
    println!("---------------------------------------------");
    println!("reading from a csv files");
    let df = CsvReader::from_path("./data/students1.csv")?
            .has_header(true)
            .finish()
            .unwrap();
    
    println!("data {:?}", df);
    let df1 = df.clone().lazy().select([
             col("SCORE"),
    ]).collect()?;
    println!("filtered column {:?}", df1);
    Ok(df)
}

fn create_new_output_csv() -> Result<(), Box<dyn Error>> {
    println!("---------------------------------------------");
    let input_dir = "./data/"; // path to input folder
    let output_file = "./output.csv"; // path to output file
    let mut writer = Writer::from_path(output_file)?;
    for entry in fs::read_dir(input_dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_file() {
            println!("Processing file ... {:?}", path);
            let mut reader = ReaderBuilder::new()
                .from_path(path)?;                
            for result in reader.records() {
                let record = result?;
                writer.write_record(&record)?;
            }
        }
    }
    println!("Created a new file {}", output_file);
    writer.flush()?;
    Ok(())
}

fn create_dataframe() -> PolarsResult<DataFrame> {
    println!("---------------------------------------------");
    let df = DataFrame::new(vec![
        Series::new("name", &["Bob", "Alice", "Charlie"]),
        Series::new("age", &[25, 30, 35]),
    ])?;
    println!("{:?}", df);

    // Search for a value that is not present in the DataFrame
    let age = 25;
    let df_sort_filter_int = df
        .clone()
        .lazy()
        .select([
            col("name").sort(Default::default()).head(None),
            col("age").filter(col("age").eq(lit(age)))
            .count(),
        ])
        .collect()
        .unwrap();
    println!("df_sort_filter_int: {:?}", df_sort_filter_int);
    
    Ok(df)
}

fn read_from_output_csv_to_df() -> PolarsResult<DataFrame> {
    println!("---------------------------------------------");
    println!("reading from file output.csv");    
    let df = CsvReader::from_path("./output.csv")?
            .has_header(false)
            .finish()
            .unwrap();
    println!("{:?}", df);
    Ok(df)
}

fn main() {
    read_csv_files().ok();
    read_csv().ok();
    create_dataframe().ok();
    create_new_output_csv().ok();
    read_from_output_csv_to_df().ok();
}
