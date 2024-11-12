use std::fs::File;
use std::io::{self, BufRead, BufReader, Read, Seek};
use flate2::read::MultiGzDecoder as GzDecoder;
use zstd::stream::read::Decoder as ZstdDecoder;
use json::JsonValue;
use std::env;
use escape_string::escape;
use getopts::Options;

fn get_names(pfx: String, obj: &JsonValue) -> Vec<String> {
    let mut ret = Vec::new();

    for (key, val) in obj.entries() {
        if val.is_array() {
            for x in 0..val.len() {
                ret.push(format!("{}{}_{}", pfx, key, x));
            }
            continue
        }

        if val.is_object() {
            let p = if pfx == "" {
                format!("{}.", key)
            } else {
                format!("{}.{}.", pfx, key)
            };

            let mut vals = get_names(p, val);
            ret.append(&mut vals);
            continue
        }

        ret.push(format!("{}{}", pfx, key));
    }

    ret
}

fn merge_vecs(vec1: Vec<String>, vec2: Vec<String>) -> Vec<String> {
    let mut ret = vec1;

    for x in vec2 {
        if !ret.contains(&x) {
            ret.push(x);
        }
    }

    ret
}

fn fopen(path: &str) -> io::Result<Box<dyn BufRead>> {
    let mut file = File::open(path)?;

    let mut buffer = [0; 4];
    file.read_exact(&mut buffer)?;
    file.seek(io::SeekFrom::Start(0))?;

    let reader: Box<dyn BufRead> = if buffer == [0x28, 0xb5, 0x2f, 0xfd] {
        Box::new(BufReader::new(ZstdDecoder::new(file)?))
    } else if buffer[0] == 0x1f && buffer[1] == 0x8b {
        Box::new(BufReader::new(GzDecoder::new(file)))
    } else {
        Box::new(BufReader::new(file))
    };

    Ok(reader)
}

fn write_header(cols: &Vec<String>) -> String {
    let mut header = String::new();
    for x in cols {
        header.push_str(&format!("\"{}\",", x))
    }
    header.pop();
    header
}

fn construct_arr(cols: &Vec<String>, key: &str, obj: &JsonValue, vals: &mut Vec<String>) {
    for x in 0..obj.len() {
        let colname = &format!("{}_{}", key, x);
        let i = match cols.iter().position(|k| k == colname) {
            Some(i) => i,
            None => {
                eprintln!("Column not found: {}", colname);
                std::process::exit(1);
            }
        };
        vals[i] = escape(&obj[x].to_string()).to_string();
    }
}

fn construct_obj(cols: &Vec<String>, key: &str, obj: &JsonValue, vals: &mut Vec<String>) {
    for (k, v) in obj.entries() {
        if v.is_array() {
            construct_arr(cols, &format!("{}.{}", key, k), v, vals);
            continue
        }

        if v.is_object() {
            construct_obj(cols, &format!("{}.{}", key, k), v, vals);
            continue
        }

        let i = match cols.iter().position(|x| x == &format!("{}.{}", key, k)) {
            Some(i) => i,
            None => {
                eprintln!("Column not found: {}.{}", key, k);
                std::process::exit(1);
            }
        };
        vals[i] = escape(&obj[i].to_string()).to_string();
    }
}

fn construct_row(cols: &Vec<String>, obj: &JsonValue, skip: &Vec<String>) -> String {
    let mut row = String::new();
    let mut vals = vec![String::new(); cols.len()];

    for (key, val) in obj.entries() {
        if skip.contains(&key.to_string()) {
            continue
        }

        if val.is_array() {
            construct_arr(cols, key, val, &mut vals);
            continue
        }

        if val.is_object() {
            construct_obj(cols, key, val, &mut vals);
            continue
        }

        let i = cols.iter().position(|x| x == key).unwrap();
        vals[i] = val.to_string();
    }

    for x in vals {
        row.push_str(&format!("\"{}\",", escape(&x)));
    }

    row.pop();
    row
}

fn usage(program: &str, opts: Options) {
    let brief = format!("Usage: {} [options] <file>", program);
    print!("{}", opts.usage(&brief));
}

struct Opts {
    skip_field: Vec<String>,
    file: String,
}

fn options() -> Option<Opts> {
    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();

    let mut skip_field: Vec<String> = Vec::new();

    let mut opts = Options::new();
    opts.optopt("s", "skip-field", "skip field", "FIELD");
    opts.optflag("h", "help", "print this help menu");
    let matches = match opts.parse(&args[1..]) {
        Ok(m) => m,
        Err(f) => {
            eprintln!("Error: {}", f.to_string());
            std::process::exit(1);
        }
    };
    if matches.opt_present("h") {
        usage(&program, opts);
        return None;
    }
    if matches.opt_present("s") {
        skip_field = matches.opt_strs("s");
    }
    let file = if !matches.free.is_empty() {
        matches.free[0].clone()
    } else {
        eprintln!("No file specified");
        std::process::exit(1);
    };

    Some(Opts {
        skip_field,
        file,
    })
}

fn main() -> io::Result<()> {
    let opts = match options() {
        Some(opts) => opts,
        None => return Ok(()),
    };

    let reader = fopen(&opts.file)?;

    // initial sweep
    let mut cols = Vec::new();
    for line in reader.lines() {
        let l = match line {
            Ok(line) => line,
            Err(err) => {
                eprintln!("Error reading file: {}", err);
                std::process::exit(1);
            }
        };
        let json: JsonValue = match json::parse(&l) {
            Ok(json) => json,
            Err(err) => {
                eprintln!("Error parsing JSON: {}", err);
                std::process::exit(1);
            }
        };
        let x = get_names("".to_string(), &json);

        cols = merge_vecs(cols, x);
    }

    if !opts.skip_field.is_empty() {
        cols = cols.into_iter().filter(|x| !opts.skip_field.contains(x)).collect();
    }

    println!("{}", write_header(&cols));
    let reader = fopen(&opts.file)?;

    for line in reader.lines() {
        let json: JsonValue = json::parse(&line?).unwrap();
        println!("{}", construct_row(&cols, &json, &opts.skip_field));
    }

    Ok(())
}

