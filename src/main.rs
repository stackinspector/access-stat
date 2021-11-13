use std::{
    collections::HashMap,
    fmt::{Display, Formatter},
    fs::File,
    io::{BufRead, BufReader, Write},
};
use serde::{Deserialize, Serialize};
use structopt::StructOpt;

#[derive(StructOpt)]
struct Args {
    #[structopt(short = "i", long)]
    input: String,
    #[structopt(short = "o", long)]
    output: String,
    #[structopt(long)]
    ip: String,
    #[structopt(long)]
    max: usize,
}

#[derive(Clone, Deserialize)]
struct Record {
    time_iso8601: String,
    remote_addr: String,
    remote_user: String,
    request: String,
    http_referer: String,
    http_user_agent: String,
    http_accept: String,
    http_x_forwarded_for: String,
    http_cookie: String,
    status: String,
    bytes_sent: String,
    body_bytes_sent: String,
    connection: String,
    connection_requests: String,
}

#[derive(PartialEq, Eq, Hash, Serialize)]
struct Payload {
    request: String,
    http_referer: String,
    http_user_agent: String,
    http_accept: String,
    http_x_forwarded_for: String,
    http_cookie: String,
}

impl Payload {
    fn from_record(record: Record) -> Payload {
        Payload {
            request: record.request,
            http_referer: record.http_referer,
            http_user_agent: record.http_user_agent,
            http_accept: record.http_accept,
            http_x_forwarded_for: record.http_x_forwarded_for,
            http_cookie: record.http_cookie,
        }
    }
}

impl Display for Payload {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}

#[derive(Serialize)]
struct Output {
    payload: Payload,
    count: usize,
    earliest: String,
    latest: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let Args { input, output, ip, max } = Args::from_args();

    let input = File::open(input)?;
    let lines = BufReader::new(input).lines();
    let mut result: HashMap<Payload, Vec<String>> = HashMap::new();

    for line in lines {
        let record: Record = serde_json::from_str(line?.as_str())?;
        if record.remote_addr == ip {
            let time = record.time_iso8601.clone();
            (*result
                .entry(Payload::from_record(record))
                .or_insert(Vec::new()))
            .push(time);
        }
    }

    let result: Vec<_> = result
        .into_iter()
        .filter(|(_, v)| v.len() >= max)
        .map(|(k, v)| Output {
            payload: k,
            count: v.len(),
            earliest: v.first().unwrap().to_owned(),
            latest: v.last().unwrap().to_owned(),
        })
        .collect();

    let mut output = File::create(output)?;
    output.write_all(serde_json::to_string_pretty(&result)?.as_bytes())?;

    Ok(())
}
