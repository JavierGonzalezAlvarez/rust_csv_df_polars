use polars::prelude::*;
use std::fs;
use std::error::Error;
use csv::{ReaderBuilder, Writer};
use std::fs::OpenOptions;
use std::io::prelude::*;

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
        let mut reader= ReaderBuilder::new()
            .has_headers(true)
            .from_path(file)?;
        println!("reader => {:?}", reader);

        for result in reader.records() {
            let record = result?;
            println!("record => {:?}", record);
            let value1: &str = record.get(0).unwrap_or("");
            let value2: &str = record.get(1).unwrap_or("");
            println!("Value at index 0 in the StringRecord: {}", value1);
            println!("Value at index 1 in the StringRecord: {}", value2);
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
    let output_file = "./output1.csv"; // path to output file
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

fn create_new_output_csv_with_header() -> Result<(), Box<dyn Error>> {
    println!("---------------------------------------------");
    /*
        read from several csv files and save it in a new csv with headers
     */
    let file_path = "./output2.csv";
    let headers: String = String::from("SCORE,POINT_AVERAGE");
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .append(true)
        .open(file_path)
        .unwrap();

    // Write the header to the file
    writeln!(file, "{}", headers).unwrap();

    let input_dir = "./data/"; // path to input folder
    let output_file = "./output2.csv"; // path to output file
    for entry in fs::read_dir(input_dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_file() {
            println!("Processing file ... {:?}", path);
            let mut reader = ReaderBuilder::new()
                .from_path(path)?;                
            for result in reader.records() {
                let record = result?;
                //writeln!(file, "{:#?}", record).unwrap();
                let value1: &str = record.get(0).unwrap_or("");
                let value2: &str = record.get(1).unwrap_or("");
                writeln!(file, "{}, {}", value1, value2).unwrap();
            }
        }
    }
    println!("Created a new file {}", output_file);
    Ok(())
}


fn insert_a_row() -> std::io::Result<()> {
    let file_path = "./output3.csv";
    let headers: String = String::from("SCORE,POINT_AVERAGE");
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open(file_path)
        .unwrap();

    // Write the line to the file
    writeln!(file, "{}", headers).unwrap();
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
    println!("reading from file output2.csv");    
    let df= CsvReader::from_path("./output2.csv")?
            .has_header(true)
            .finish()
            .unwrap();
    println!("{:?}", df);
    Ok(df)
}

fn delete_duplicated_rows_by_column() -> PolarsResult<DataFrame>  {
    println!("---------------------------------------------");
    println!("reading from a csv file and getting duplicated rows");
    let df = CsvReader::from_path("./output2.csv")?
            .has_header(true)
            .finish()
            .unwrap();    
    let unique: ChunkedArray<BooleanType>  = df.is_duplicated().unwrap();

    println!("there are duplicated rows{:?}", unique);    

    // polars verson : "0.27.2"
    //let df_unique = df.unique(Some(&["SCORE".to_string()]), UniqueKeepStrategy::First);

    /* polars version:  "0.28.0"
        pub fn unique(
            &self,
            subset: Option<&[String]>,
            keep: UniqueKeepStrategy,
            slice: Option<(i64, usize)>
        ) -> Result<DataFrame, PolarsError>
     */
    //let df_unique = df.unique(Some(&["SCORE".to_string()]), UniqueKeepStrategy::First, Some((1 ,1)));
    let df_unique = df.unique(Some(&["SCORE".to_string()]), UniqueKeepStrategy::First, None);
    println!("dataframe without duplicated values of a column {:?}", df_unique);

    Ok(df)
}

fn main() {
    read_csv_files().ok();
    read_csv().ok();
    create_dataframe().ok();
    insert_a_row().ok();
    create_new_output_csv().ok();
    create_new_output_csv_with_header().ok();
    read_from_output_csv_to_df().ok();
    delete_duplicated_rows_by_column().ok();
}

/*
output

Files name .csv => ["./data/students1.csv", "./data/students2.csv"]
reader => Reader { core: Reader { dfa: Dfa(N/A), dfa_state: DfaState(0), nfa_state: StartRecord, delimiter: 44, 
    term: CRLF, quote: 34, escape: None, double_quote: true, comment: None, quoting: true, use_nfa: false, 
    line: 1, has_read: false, output_pos: 0 }, rdr: BufReader { reader: File { fd: 3, path: "/data/students1.csv", read: true, write: false },
     buffer: 0/8192 }, state: ReaderState { headers: None, has_headers: true, flexible: false, trim: None, first_field_count: None, 
        cur_pos: Position { byte: 0, line: 1, record: 0 }, first: false, seeked: false, eof: NotEof } }
record => StringRecord(["41714", "12.4"])
Value at index 0 in the StringRecord: 41714
Value at index 1 in the StringRecord: 12.4
record => StringRecord(["41664", "12.52"])
Value at index 0 in the StringRecord: 41664
Value at index 1 in the StringRecord: 12.52
record => StringRecord(["41760", "12.54"])
Value at index 0 in the StringRecord: 41760
Value at index 1 in the StringRecord: 12.54
record => StringRecord(["41685", "12.74"])
Value at index 0 in the StringRecord: 41685
Value at index 1 in the StringRecord: 12.74
record => StringRecord(["41693", "12.83"])
Value at index 0 in the StringRecord: 41693
Value at index 1 in the StringRecord: 12.83
record => StringRecord(["41670", "12.91"])
Value at index 0 in the StringRecord: 41670
Value at index 1 in the StringRecord: 12.91
record => StringRecord(["41764", "143"])
Value at index 0 in the StringRecord: 41764
Value at index 1 in the StringRecord: 143
record => StringRecord(["41764", "133"])
Value at index 0 in the StringRecord: 41764
Value at index 1 in the StringRecord: 133
reader => Reader { core: Reader { dfa: Dfa(N/A), dfa_state: DfaState(0), nfa_state: StartRecord, delimiter: 44,
     term: CRLF, quote: 34, escape: None, double_quote: true, comment: None, quoting: true, use_nfa: false, line: 1, 
     has_read: false, output_pos: 0 }, rdr: BufReader { reader: File { fd: 3, path: "/data/students2.csv", 
     read: true, write: false }, buffer: 0/8192 }, state: ReaderState { headers: None, has_headers: true, 
        flexible: false, trim: None, first_field_count: None, cur_pos: Position { byte: 0, line: 1, record: 0 }, 
        first: false, seeked: false, eof: NotEof } }
record => StringRecord(["1714", "2.4"])
Value at index 0 in the StringRecord: 1714
Value at index 1 in the StringRecord: 2.4
record => StringRecord(["1664", "2.52"])
Value at index 0 in the StringRecord: 1664
Value at index 1 in the StringRecord: 2.52
record => StringRecord(["1760", "2.54"])
Value at index 0 in the StringRecord: 1760
Value at index 1 in the StringRecord: 2.54
record => StringRecord(["1685", "2.74"])
Value at index 0 in the StringRecord: 1685
Value at index 1 in the StringRecord: 2.74
record => StringRecord(["1693", "2.83"])
Value at index 0 in the StringRecord: 1693
Value at index 1 in the StringRecord: 2.83
record => StringRecord(["1670", "2.91"])
Value at index 0 in the StringRecord: 1670
Value at index 1 in the StringRecord: 2.91
record => StringRecord(["1764", "3"])
Value at index 0 in the StringRecord: 1764
Value at index 1 in the StringRecord: 3
record => StringRecord(["1764", "32"])
Value at index 0 in the StringRecord: 1764
Value at index 1 in the StringRecord: 32
---------------------------------------------
reading from a csv files
data shape: (8, 2)
┌───────┬───────────────┐
│ SCORE ┆ POINT_AVERAGE │
│ ---   ┆ ---           │
│ i64   ┆ f64           │
╞═══════╪═══════════════╡
│ 41714 ┆ 12.4          │
│ 41664 ┆ 12.52         │
│ 41760 ┆ 12.54         │
│ 41685 ┆ 12.74         │
│ 41693 ┆ 12.83         │
│ 41670 ┆ 12.91         │
│ 41764 ┆ 143.0         │
│ 41764 ┆ 133.0         │
└───────┴───────────────┘
filtered column shape: (8, 1)
┌───────┐
│ SCORE │
│ ---   │
│ i64   │
╞═══════╡
│ 41714 │
│ 41664 │
│ 41760 │
│ 41685 │
│ 41693 │
│ 41670 │
│ 41764 │
│ 41764 │
└───────┘
---------------------------------------------
shape: (3, 2)
┌─────────┬─────┐
│ name    ┆ age │
│ ---     ┆ --- │
│ str     ┆ i32 │
╞═════════╪═════╡
│ Bob     ┆ 25  │
│ Alice   ┆ 30  │
│ Charlie ┆ 35  │
└─────────┴─────┘
df_sort_filter_int: shape: (3, 2)
┌─────────┬─────┐
│ name    ┆ age │
│ ---     ┆ --- │
│ str     ┆ u32 │
╞═════════╪═════╡
│ Alice   ┆ 1   │
│ Bob     ┆ 1   │
│ Charlie ┆ 1   │
└─────────┴─────┘
---------------------------------------------
Processing file ... "./data/students1.csv"
Processing file ... "./data/students2.csv"
Created a new file ./output1.csv
---------------------------------------------
Processing file ... "./data/students1.csv"
Processing file ... "./data/students2.csv"
Created a new file ./output2.csv
---------------------------------------------
reading from file output2.csv
shape: (67, 2)
┌───────┬───────────────┐
│ SCORE ┆ POINT_AVERAGE │
│ ---   ┆ ---           │
│ str   ┆ str           │
╞═══════╪═══════════════╡
│ 41714 ┆  12.4         │
│ 41664 ┆  12.52        │
│ 41760 ┆  12.54        │
│ 41685 ┆  12.74        │
│ …     ┆ …             │
│ 1693  ┆  2.83         │
│ 1670  ┆  2.91         │
│ 1764  ┆  3            │
│ 1764  ┆  32           │
└───────┴───────────────┘
---------------------------------------------
reading from a csv file and getting duplicated rows
there are duplicated rowsshape: (67,)
ChunkedArray: '' [bool]
[
        true
        true
        true
        true
        true
        true
        true
        true
        true
        true
        true
        true
        …
        true
        true
        true
        true
        true
        true
        true
        true
        true
        true
        true
        true
        true
]
dataframe without duplicated values of a column Ok(shape: (15, 2)
┌───────┬───────────────┐
│ SCORE ┆ POINT_AVERAGE │
│ ---   ┆ ---           │
│ str   ┆ str           │
╞═══════╪═══════════════╡
│ 1760  ┆  2.54         │
│ 1664  ┆  2.52         │
│ 1685  ┆  2.74         │
│ 41764 ┆  143          │
│ …     ┆ …             │
│ 1764  ┆  3            │
│ 41670 ┆  12.91        │
│ 1670  ┆  2.91         │
│ 1693  ┆  2.83         │
└───────┴───────────────┘)
 */