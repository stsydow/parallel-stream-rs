use std::error::Error;
use std::fmt;
use std::fs::OpenOptions;
use std::io;
use std::io::prelude::*;
use std::process::exit;

use log::{LevelFilter, SetLoggerError};
use simplelog::TerminalMode;

#[derive(Debug)]
pub enum LoggingError {
    Io(io::Error),
    Init(SetLoggerError),
    TerminalError,
}

impl fmt::Display for LoggingError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            LoggingError::Io(ref err) => write!(f, "IO error: {}", err),
            LoggingError::Init(ref err) => write!(f, "set_logger error: {}", err),
            LoggingError::TerminalError => write!(f, "missing terminal error"),
        }
    }
}

impl Error for LoggingError {
    /*
    fn description(&self) -> &str {
        match *self {
            LoggingError::Io(ref err) => err.description(),
            LoggingError::Init(ref err) => err.description(),
            LoggingError::TerminalError => "missing terminal error",
        }
    }
    */

    fn cause(&self) -> Option<&dyn Error> {
        match *self {
            LoggingError::Io(ref err) => Some(err),
            LoggingError::Init(ref err) => Some(err),
            LoggingError::TerminalError => None,
        }
    }
}

impl From<SetLoggerError> for LoggingError {
    fn from(err: SetLoggerError) -> LoggingError {
        LoggingError::Init(err)
    }
}

impl From<io::Error> for LoggingError {
    fn from(err: io::Error) -> LoggingError {
        LoggingError::Io(err)
    }
}

pub fn set_logger(log_stream: &str, log_level: LevelFilter) -> Result<(), LoggingError> {
    let log_conf = simplelog::Config::default();

    let logger: Box<dyn simplelog::SharedLogger> = if log_stream == "-" {
        match simplelog::TermLogger::new(log_level, log_conf, TerminalMode::Stderr) {
            Some(logger) => Ok(logger),
            None => Err(LoggingError::TerminalError),
        }?
    } else {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(log_stream)?;
        simplelog::WriteLogger::new(log_level, log_conf, file)
    };

    simplelog::CombinedLogger::init(vec![logger])?;

    Ok(())
}

pub fn set_logger_or_exit(log_stream: &str, log_level: LevelFilter) {
    let res = set_logger(&log_stream, log_level);
    if res.is_err() {
        let err = res.unwrap_err();
        let stderr = io::stderr();
        let _ = writeln!(
            stderr.lock(),
            "can't start logging to \"{}\": {}",
            log_stream,
            err
        );
        exit(-1);
    }
}
