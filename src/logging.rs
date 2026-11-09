use clap::Args;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::{
    fmt::{self, Write as _},
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicI64, AtomicU64, Ordering},
    },
    thread::JoinHandle,
    time::Duration,
};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tracing::{
    Event, Subscriber,
    field::{Field, Visit},
};
use tracing_subscriber::{Layer, layer::Context, prelude::*, registry::LookupSpan};
use uuid::Uuid;

const MAX_RECORD_BYTES: usize = 64 * 1024;
const MAX_FIELD_BYTES: usize = 4096;

#[derive(Args, Debug)]
pub(crate) struct Options {
    #[arg(long, env = "ETCQLITE_LOG_DIR")]
    pub log_dir: Option<PathBuf>,
    #[arg(long, env = "ETCQLITE_LOG_MAX_BYTES", default_value_t = 10 * 1024 * 1024, value_parser = clap::value_parser!(u64).range(1..))]
    pub log_max_bytes: u64,
    #[arg(long, env = "ETCQLITE_LOG_RETENTION_DAYS", default_value_t = 30, value_parser = clap::value_parser!(u32).range(1..=36500))]
    pub log_retention_days: u32,
    #[arg(long, env = "ETCQLITE_NO_FILE_LOG", conflicts_with = "log_dir")]
    pub no_file_log: bool,
}

impl Options {
    pub fn file_options(&self, data_dir: &Path) -> io::Result<Option<FileOptions>> {
        if self.no_file_log {
            return Ok(None);
        }
        let base = data_dir.canonicalize()?;
        if !base.join("node.db").is_file() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "file logging requires an initialized node; run init first",
            ));
        }
        let directory = match &self.log_dir {
            Some(path) if path.is_absolute() => path.clone(),
            Some(path) => base.join(path),
            None => base.join("logs"),
        };
        Ok(Some(FileOptions {
            directory,
            max_bytes: self.log_max_bytes,
            retention_days: self.log_retention_days,
        }))
    }
}

#[derive(Clone)]
pub(crate) struct FileOptions {
    pub directory: PathBuf,
    pub max_bytes: u64,
    pub retention_days: u32,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Identity {
    format: u32,
    stream_id: String,
}

struct FileState {
    file: Option<File>,
    last_record: Option<OffsetDateTime>,
    directory_lock: Option<File>,
    closed: bool,
}

struct FileLog {
    options: FileOptions,
    stream_id: String,
    state: Mutex<FileState>,
}

fn invalid(message: impl fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.to_string())
}

fn private_regular(path: &Path, directory: &Path) -> io::Result<fs::Metadata> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() {
        return Err(invalid(format!(
            "log path is not a regular non-symlink file: {}",
            path.display()
        )));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        if metadata.permissions().mode() & 0o077 != 0
            || metadata.nlink() != 1
            || metadata.uid() != fs::metadata(directory)?.uid()
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!(
                    "log must be private, singly linked and owned by its directory owner: {}",
                    path.display()
                ),
            ));
        }
    }
    Ok(metadata)
}

fn open_private(path: &Path, directory: &Path) -> io::Result<File> {
    match fs::symlink_metadata(path) {
        Ok(_) => {
            private_regular(path, directory)?;
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options.open(path)?;
    let after = private_regular(path, directory)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let opened = file.metadata()?;
        if opened.ino() != after.ino() || opened.dev() != after.dev() {
            return Err(invalid("log file identity changed during open"));
        }
    }
    #[cfg(not(unix))]
    let _ = after;
    Ok(file)
}

impl FileLog {
    fn open(options: FileOptions, now: OffsetDateTime) -> io::Result<Self> {
        if options.max_bytes == 0 || options.retention_days == 0 || options.retention_days > 36500 {
            return Err(invalid(
                "log size and retention must be positive and bounded",
            ));
        }
        let mut builder = fs::DirBuilder::new();
        builder.recursive(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
            builder.mode(0o700);
            builder.create(&options.directory)?;
            let metadata = fs::symlink_metadata(&options.directory)?;
            if !metadata.is_dir()
                || metadata.file_type().is_symlink()
                || metadata.permissions().mode() & 0o077 != 0
            {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "log directory must be a private non-symlink directory (mode 0700)",
                ));
            }
        }
        #[cfg(not(unix))]
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "private file logging is qualified only on macOS/Linux",
        ));

        let directory = &options.directory;
        let lock = open_private(&directory.join(".etcqlite-log.lock"), directory)?;
        lock.try_lock().map_err(|error| {
            io::Error::new(
                io::ErrorKind::WouldBlock,
                format!("log directory already has a writer or cannot be locked: {error}"),
            )
        })?;
        let identity_path = directory.join(".etcqlite-log.json");
        let mut identity_file = open_private(&identity_path, directory)?;
        let identity = if identity_file.metadata()?.len() == 0 {
            if directory.join("app.log").exists() {
                return Err(invalid(
                    "refusing to adopt app.log without its logging identity",
                ));
            }
            let identity = Identity {
                format: 1,
                stream_id: Uuid::new_v4().simple().to_string(),
            };
            identity_file.write_all(&serde_json::to_vec(&identity)?)?;
            identity_file.sync_all()?;
            identity
        } else {
            let mut bytes = Vec::new();
            identity_file.take(4097).read_to_end(&mut bytes)?;
            if bytes.len() > 4096 {
                return Err(invalid("log identity exceeds 4096 bytes"));
            }
            serde_json::from_slice::<Identity>(&bytes).map_err(invalid)?
        };
        if identity.format != 1
            || identity.stream_id.len() != 32
            || !identity
                .stream_id
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(invalid("invalid log stream identity"));
        }
        let mut file = open_private(&directory.join("app.log"), directory)?;
        let last_record = Self::last_record(&mut file)?;
        let log = Self {
            options,
            stream_id: identity.stream_id,
            state: Mutex::new(FileState {
                file: Some(file),
                last_record,
                directory_lock: Some(lock),
                closed: false,
            }),
        };
        log.maintain(now)?;
        Ok(log)
    }

    fn state(&self) -> io::Result<std::sync::MutexGuard<'_, FileState>> {
        self.state
            .lock()
            .map_err(|_| io::Error::other("log writer mutex poisoned"))
    }

    fn last_record(file: &mut File) -> io::Result<Option<OffsetDateTime>> {
        let len = file.metadata()?.len();
        if len == 0 {
            return Ok(None);
        }
        let mut tail = Vec::new();
        file.seek(SeekFrom::Start(
            len.saturating_sub(MAX_RECORD_BYTES as u64 + 1),
        ))?;
        file.read_to_end(&mut tail)?;
        if !tail.ends_with(b"\n") {
            return Err(invalid(
                "incomplete active log record; preserve the log and select a new log directory before restart",
            ));
        }
        let line = tail[..tail.len() - 1]
            .rsplit(|byte| *byte == b'\n')
            .next()
            .ok_or_else(|| invalid("active log has no complete record"))?;
        let value: Value = serde_json::from_slice(line).map_err(invalid)?;
        if value["etcqlite_log"] != 1 {
            return Err(invalid("active log is not an owned Etcqlite stream"));
        }
        let timestamp = value["timestamp"]
            .as_str()
            .ok_or_else(|| invalid("active log is missing its record timestamp"))?;
        Ok(Some(
            OffsetDateTime::parse(timestamp, &Rfc3339).map_err(invalid)?,
        ))
    }

    fn archive_path(&self, timestamp: OffsetDateTime) -> PathBuf {
        self.options.directory.join(format!(
            "app.{}.{}.{}.log",
            self.stream_id,
            timestamp.unix_timestamp_nanos(),
            Uuid::new_v4().simple()
        ))
    }

    fn rotate(&self, state: &mut FileState, now: OffsetDateTime) -> io::Result<()> {
        let Some(timestamp) = state.last_record else {
            return Ok(());
        };
        if let Some(file) = state.file.take() {
            file.sync_all()?;
        }
        let live = self.options.directory.join("app.log");
        private_regular(&live, &self.options.directory)?;
        let archive = loop {
            let path = self.archive_path(timestamp);
            match fs::symlink_metadata(&path) {
                Err(error) if error.kind() == io::ErrorKind::NotFound => break path,
                Ok(_) => {}
                Err(error) => return Err(error),
            }
        };
        fs::rename(&live, &archive).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!(
                    "rotate {} to {}: {error}",
                    live.display(),
                    archive.display()
                ),
            )
        })?;
        state.last_record = None;
        state.file = Some(open_private(&live, &self.options.directory)?);
        File::open(&self.options.directory)?.sync_all()?;
        self.retain(now)
    }

    fn retain(&self, now: OffsetDateTime) -> io::Result<()> {
        let cutoff = now - time::Duration::days(i64::from(self.options.retention_days));
        let prefix = format!("app.{}.", self.stream_id);
        let mut failures = Vec::new();
        let mut failure_count = 0;
        for entry in fs::read_dir(&self.options.directory)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            let Some(rest) = name
                .strip_prefix(&prefix)
                .and_then(|name| name.strip_suffix(".log"))
            else {
                continue;
            };
            let Some((timestamp, nonce)) = rest.split_once('.') else {
                continue;
            };
            if nonce.len() != 32 || !nonce.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                continue;
            }
            let Some(timestamp) = timestamp
                .parse::<i128>()
                .ok()
                .and_then(|value| OffsetDateTime::from_unix_timestamp_nanos(value).ok())
            else {
                continue;
            };
            if timestamp >= cutoff {
                continue;
            }
            let path = entry.path();
            match private_regular(&path, &self.options.directory) {
                Ok(_) => {}
                Err(error) if error.kind() == io::ErrorKind::InvalidData => {
                    continue;
                }
                Err(error) => {
                    failure_count += 1;
                    if failures.len() < 32 {
                        failures.push(format!("inspect {}: {error}", path.display()));
                    }
                    continue;
                }
            }
            if let Err(error) = fs::remove_file(&path) {
                failure_count += 1;
                if failures.len() < 32 {
                    failures.push(format!("remove {}: {error}", path.display()));
                }
            }
        }
        if failure_count != 0 {
            return Err(io::Error::other(format!(
                "log retention incomplete ({failure_count} failures): {}",
                failures.join("; ")
            )));
        }
        Ok(())
    }

    fn maintain(&self, now: OffsetDateTime) -> io::Result<()> {
        let mut state = self.state()?;
        if state
            .last_record
            .is_some_and(|last| last.date() != now.date())
        {
            self.rotate(&mut state, now)?;
        }
        self.retain(now)
    }

    fn append(&self, bytes: &[u8], now: OffsetDateTime) -> io::Result<()> {
        if bytes.len() > MAX_RECORD_BYTES
            || bytes.len() as u64 > self.options.max_bytes
            || !bytes.ends_with(b"\n")
        {
            return Err(invalid(
                "encoded log record exceeds its byte cap or lacks a complete newline",
            ));
        }
        let mut state = self.state()?;
        if state.closed {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "log writer is shut down",
            ));
        }
        let live = self.options.directory.join("app.log");
        if state.file.is_none() {
            let mut file = open_private(&live, &self.options.directory)?;
            state.last_record = Self::last_record(&mut file)?;
            state.file = Some(file);
        }
        let metadata = private_regular(&live, &self.options.directory)?;
        #[cfg(unix)]
        if let Some(file) = &state.file {
            use std::os::unix::fs::MetadataExt;
            let opened = file.metadata()?;
            if opened.ino() != metadata.ino() || opened.dev() != metadata.dev() {
                state.file = None;
                return Err(invalid(
                    "active log identity changed; external rotation is not supported",
                ));
            }
        }
        let length = metadata.len();
        if state
            .last_record
            .is_some_and(|last| last.date() != now.date())
            || length.saturating_add(bytes.len() as u64) > self.options.max_bytes
        {
            self.rotate(&mut state, now)?;
        }
        let file = state
            .file
            .as_mut()
            .ok_or_else(|| io::Error::other("rotation did not reopen the live log"))?;
        file.seek(SeekFrom::End(0))?;
        if let Err(error) = file.write_all(bytes) {
            state.file = None;
            return Err(io::Error::new(
                error.kind(),
                format!("append {}: {error}", live.display()),
            ));
        }
        state.last_record = Some(now);
        Ok(())
    }

    fn flush(&self) -> io::Result<()> {
        if let Some(file) = self.state()?.file.as_mut() {
            file.sync_all()?;
        }
        Ok(())
    }

    fn close(&self) -> io::Result<()> {
        let mut state = self.state()?;
        state.closed = true;
        let result = state.file.take().map(|file| file.sync_all()).transpose();
        state.directory_lock.take();
        result.map(|_| ())
    }
}

#[derive(Clone)]
struct Redactor {
    patterns: Vec<(Regex, &'static str)>,
}

impl Redactor {
    fn new() -> Self {
        Self { patterns: vec![
            (Regex::new(r"(?s)-----BEGIN [^-]*PRIVATE KEY-----.*?(?:-----END [^-]*PRIVATE KEY-----|$)").expect("fixed PEM pattern"), "[REDACTED]"),
            (Regex::new(r"(?i)(https?://)[^/@\s]+@").expect("fixed URL pattern"), "${1}[REDACTED]@"),
            (Regex::new(r"(?i)(authorization\s*[:=]\s*)[^\r\n,]+").expect("fixed authorization pattern"), "${1}[REDACTED]"),
            (Regex::new(r#"(?i)((?:password|passwd|token|secret|api[_-]?key|credential|cookie)["']?\s*[:=]\s*)(?:"[^"]*"|'[^']*'|[^\s,;}\]&]+)"#).expect("fixed secret pattern"), "${1}[REDACTED]"),
        ] }
    }

    fn sensitive(&self, field: &str) -> bool {
        let key: String = field
            .chars()
            .filter(char::is_ascii_alphanumeric)
            .flat_map(char::to_lowercase)
            .collect();
        [
            "authorization",
            "password",
            "passwd",
            "token",
            "secret",
            "credential",
            "privatekey",
            "cookie",
        ]
        .iter()
        .any(|needle| key.contains(needle))
            || matches!(
                key.as_str(),
                "authorization"
                    | "password"
                    | "passwd"
                    | "passwordhash"
                    | "token"
                    | "accesstoken"
                    | "refreshtoken"
                    | "secret"
                    | "clientsecret"
                    | "apikey"
                    | "credential"
                    | "credentials"
                    | "privatekey"
                    | "cookie"
                    | "setcookie"
                    | "key"
                    | "keys"
                    | "value"
                    | "values"
                    | "keyvalues"
                    | "payload"
                    | "request"
                    | "response"
                    | "body"
                    | "headers"
            )
    }

    fn text(&self, text: &str) -> String {
        let mut text = text.to_owned();
        for (pattern, replacement) in &self.patterns {
            text = pattern.replace_all(&text, *replacement).into_owned();
        }
        text
    }
}

struct BoundedText {
    text: String,
    truncated: bool,
}

impl fmt::Write for BoundedText {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        let mut allowed = value
            .len()
            .min(MAX_FIELD_BYTES.saturating_sub(self.text.len()));
        while !value.is_char_boundary(allowed) {
            allowed -= 1;
        }
        self.text.push_str(&value[..allowed]);
        self.truncated |= allowed < value.len();
        Ok(())
    }
}

struct Visitor<'a> {
    fields: Map<String, Value>,
    redactor: &'a Redactor,
}

impl Visit for Visitor<'_> {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if self.fields.len() >= 32 {
            self.fields
                .insert("logging_fields_truncated".into(), Value::Bool(true));
            return;
        }
        let value = if self.redactor.sensitive(field.name()) {
            "[REDACTED]".to_owned()
        } else {
            let mut text = BoundedText {
                text: String::new(),
                truncated: false,
            };
            if write!(&mut text, "{value:?}").is_err() {
                text.text.push_str(" [formatting failed]");
            }
            let mut value = self.redactor.text(&text.text);
            if text.truncated {
                value.push_str(" [truncated]");
            }
            value
        };
        self.fields
            .insert(field.name().to_owned(), Value::String(value));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.record_debug(field, &format_args!("{value}"));
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        if self.redactor.sensitive(field.name()) {
            self.record_str(field, "[REDACTED]");
        } else {
            self.fields.insert(field.name().to_owned(), value.into());
        }
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        if self.redactor.sensitive(field.name()) {
            self.record_str(field, "[REDACTED]");
        } else {
            self.fields.insert(field.name().to_owned(), value.into());
        }
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        if self.redactor.sensitive(field.name()) {
            self.record_str(field, "[REDACTED]");
        } else {
            self.fields.insert(field.name().to_owned(), value.into());
        }
    }
}

#[derive(Clone)]
struct SpanFields(Map<String, Value>);

struct LoggingLayer {
    file: Option<Arc<FileLog>>,
    redactor: Redactor,
}

fn diagnostic(error: impl fmt::Display) {
    static LAST: AtomicI64 = AtomicI64::new(i64::MIN / 2);
    static SUPPRESSED: AtomicU64 = AtomicU64::new(0);
    let now = OffsetDateTime::now_utc().unix_timestamp();
    let previous = LAST.load(Ordering::Relaxed);
    if now.saturating_sub(previous) >= 60
        && LAST
            .compare_exchange(previous, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
    {
        let suppressed = SUPPRESSED.swap(0, Ordering::Relaxed);
        eprintln!(
            "etcqlite logging failure (file output may be incomplete; {suppressed} repeated diagnostics suppressed): {error}"
        );
    } else {
        SUPPRESSED.fetch_add(1, Ordering::Relaxed);
    }
}

impl<S> Layer<S> for LoggingLayer
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn on_new_span(
        &self,
        attributes: &tracing::span::Attributes<'_>,
        id: &tracing::Id,
        context: Context<'_, S>,
    ) {
        let mut visitor = Visitor {
            fields: Map::new(),
            redactor: &self.redactor,
        };
        attributes.record(&mut visitor);
        if let Some(span) = context.span(id) {
            span.extensions_mut().insert(SpanFields(visitor.fields));
        }
    }

    fn on_record(
        &self,
        id: &tracing::Id,
        values: &tracing::span::Record<'_>,
        context: Context<'_, S>,
    ) {
        let mut visitor = Visitor {
            fields: Map::new(),
            redactor: &self.redactor,
        };
        values.record(&mut visitor);
        if let Some(span) = context.span(id)
            && let Some(fields) = span.extensions_mut().get_mut::<SpanFields>()
        {
            fields.0.extend(visitor.fields);
        }
    }

    fn on_event(&self, event: &Event<'_>, context: Context<'_, S>) {
        let now = OffsetDateTime::now_utc();
        let mut visitor = Visitor {
            fields: Map::new(),
            redactor: &self.redactor,
        };
        event.record(&mut visitor);
        let mut record = serde_json::json!({
            "etcqlite_log": 1, "timestamp": now.format(&Rfc3339).expect("UTC timestamp"),
            "level": event.metadata().level().as_str(), "target": event.metadata().target(), "fields": visitor.fields,
        });
        if let Some(scope) = context.event_scope(event) {
            let spans: Vec<Value> = scope
                .from_root()
                .take(8)
                .map(|span| {
                    let fields = span
                        .extensions()
                        .get::<SpanFields>()
                        .map(|fields| fields.0.clone())
                        .unwrap_or_default();
                    serde_json::json!({"name":span.name(),"fields":fields})
                })
                .collect();
            if !spans.is_empty() {
                record["spans"] = spans.into();
            }
        }
        let mut bytes = serde_json::to_vec(&record).expect("JSON log values");
        if bytes.len() >= MAX_RECORD_BYTES {
            record["fields"] = serde_json::json!({"message":"oversized log record replaced", "logging_record_truncated":true});
            record.as_object_mut().expect("log object").remove("spans");
            bytes = serde_json::to_vec(&record).expect("bounded JSON log values");
        }
        bytes.push(b'\n');
        if let Err(error) = io::stderr().lock().write_all(&bytes) {
            diagnostic(error);
        }
        if let Some(file) = &self.file
            && let Err(error) = file.append(&bytes, now)
        {
            diagnostic(error);
        }
    }
}

pub(crate) struct Guard {
    file: Option<Arc<FileLog>>,
    stop: Arc<(Mutex<bool>, Condvar)>,
    timer: Option<JoinHandle<io::Result<()>>>,
}

impl Guard {
    pub fn install(
        options: Option<FileOptions>,
        filter: tracing_subscriber::EnvFilter,
    ) -> io::Result<Self> {
        let file = options
            .map(|options| FileLog::open(options, OffsetDateTime::now_utc()).map(Arc::new))
            .transpose()?;
        tracing_subscriber::registry()
            .with(filter)
            .with(LoggingLayer {
                file: file.clone(),
                redactor: Redactor::new(),
            })
            .try_init()
            .map_err(io::Error::other)?;
        let stop = Arc::new((Mutex::new(false), Condvar::new()));
        let timer = file
            .as_ref()
            .map(|file| {
                let file = Arc::clone(file);
                let stop = Arc::clone(&stop);
                std::thread::Builder::new()
                    .name("etcqlite-log-retention".into())
                    .spawn(move || {
                        let mut day = OffsetDateTime::now_utc().date();
                        let mut stopping = stop
                            .0
                            .lock()
                            .map_err(|_| io::Error::other("log timer mutex poisoned"))?;
                        while !*stopping {
                            let (guard, _) = stop
                                .1
                                .wait_timeout(stopping, Duration::from_secs(60))
                                .map_err(|_| io::Error::other("log timer wait poisoned"))?;
                            stopping = guard;
                            if *stopping {
                                break;
                            }
                            let now = OffsetDateTime::now_utc();
                            if now.date() != day {
                                match file.maintain(now) {
                                    Ok(()) => day = now.date(),
                                    Err(error) => diagnostic(error),
                                }
                            }
                        }
                        file.flush()
                    })
            })
            .transpose()?;
        if let Some(file) = &file {
            tracing::info!(
                event = "logging_configured",
                directory = %file.options.directory.display(),
                max_bytes = file.options.max_bytes,
                retention_days = file.options.retention_days,
                timezone = "UTC"
            );
        }
        Ok(Self { file, stop, timer })
    }

    pub fn finish(mut self) -> io::Result<()> {
        *self
            .stop
            .0
            .lock()
            .map_err(|_| io::Error::other("log timer mutex poisoned"))? = true;
        self.stop.1.notify_all();
        if let Some(timer) = self.timer.take() {
            timer
                .join()
                .map_err(|_| io::Error::other("log retention thread panicked"))??;
        }
        if let Some(file) = &self.file {
            file.close()?;
        }
        Ok(())
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::{collections::BTreeSet, os::unix::fs::PermissionsExt};
    use time::macros::datetime;

    fn options(directory: &Path, max_bytes: u64) -> FileOptions {
        fs::create_dir_all(directory).unwrap();
        fs::set_permissions(directory, fs::Permissions::from_mode(0o700)).unwrap();
        FileOptions {
            directory: directory.to_owned(),
            max_bytes,
            retention_days: 30,
        }
    }

    fn record(at: OffsetDateTime, message: &str) -> Vec<u8> {
        let mut bytes = serde_json::to_vec(&serde_json::json!({
            "etcqlite_log": 1,
            "timestamp": at.format(&Rfc3339).unwrap(),
            "fields": {"message": message},
        }))
        .unwrap();
        bytes.push(b'\n');
        bytes
    }

    fn logs(path: &Path) -> Vec<PathBuf> {
        fs::read_dir(path)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "log"))
            .collect()
    }

    #[test]
    fn byte_cap_includes_utf8_and_newlines_and_allows_equality() {
        let temp = tempfile::tempdir().unwrap();
        let now = datetime!(2026-11-09 12:00 UTC);
        let bytes = record(now, "synthetic \u{00e9}");
        let cap = bytes.len() as u64 * 2;
        let log = FileLog::open(options(temp.path(), cap), now).unwrap();
        log.append(&bytes, now).unwrap();
        log.append(&bytes, now).unwrap();
        assert_eq!(
            fs::metadata(temp.path().join("app.log")).unwrap().len(),
            cap
        );
        assert_eq!(logs(temp.path()).len(), 1);
        log.append(&bytes, now).unwrap();
        assert_eq!(logs(temp.path()).len(), 2);
        for path in logs(temp.path()) {
            assert!(fs::metadata(&path).unwrap().len() <= cap);
            assert!(std::str::from_utf8(&fs::read(path).unwrap()).is_ok());
        }
    }

    #[test]
    fn midnight_and_restart_use_record_time_not_touched_mtime() {
        let temp = tempfile::tempdir().unwrap();
        let yesterday = datetime!(2026-11-08 23:59:59 UTC);
        let now = yesterday + time::Duration::seconds(2);
        let config = options(temp.path(), 4096);
        let log = FileLog::open(config.clone(), yesterday).unwrap();
        log.append(&record(yesterday, "old"), yesterday).unwrap();
        drop(log);
        let file = OpenOptions::new()
            .write(true)
            .open(temp.path().join("app.log"))
            .unwrap();
        file.set_modified(std::time::SystemTime::now()).unwrap();
        drop(file);
        let log = FileLog::open(config, now).unwrap();
        log.append(&record(now, "new"), now).unwrap();
        assert_eq!(logs(temp.path()).len(), 2);
        assert!(
            !fs::read_to_string(temp.path().join("app.log"))
                .unwrap()
                .contains("\"old\"")
        );
    }

    #[test]
    fn retention_is_strict_and_preserves_unrelated_files_and_symlinks() {
        let temp = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let now = datetime!(2026-11-09 12:00 UTC);
        let cutoff = now - time::Duration::days(30);
        let log = FileLog::open(options(temp.path(), 4096), now).unwrap();
        let old = log.archive_path(cutoff - time::Duration::seconds(1));
        let exact = log.archive_path(cutoff);
        let recent = log.archive_path(cutoff + time::Duration::seconds(1));
        for path in [&old, &exact, &recent] {
            fs::write(path, record(cutoff, "fixture")).unwrap();
            fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
        }
        let unrelated = temp.path().join("another-app.log");
        fs::write(&unrelated, b"preserve").unwrap();
        let target = outside.path().join("external.log");
        fs::write(&target, b"preserve").unwrap();
        let link = log.archive_path(cutoff - time::Duration::days(1));
        std::os::unix::fs::symlink(&target, &link).unwrap();
        log.maintain(now).unwrap();
        assert!(!old.exists());
        assert!(exact.exists() && recent.exists() && unrelated.exists());
        assert!(link.is_symlink() && target.exists());
    }

    #[test]
    fn idle_maintenance_and_retention_override_work_without_new_events() {
        let temp = tempfile::tempdir().unwrap();
        let old = datetime!(2026-11-01 12:00 UTC);
        let now = old + time::Duration::days(8);
        let mut config = options(temp.path(), 4096);
        config.retention_days = 7;
        let log = FileLog::open(config, old).unwrap();
        log.append(&record(old, "old"), old).unwrap();
        log.maintain(now).unwrap();
        assert_eq!(logs(temp.path()), vec![temp.path().join("app.log")]);
        assert_eq!(fs::metadata(temp.path().join("app.log")).unwrap().len(), 0);
    }

    #[test]
    fn concurrent_producers_never_lose_duplicate_or_split_records() {
        let temp = tempfile::tempdir().unwrap();
        let now = datetime!(2026-11-09 12:00 UTC);
        let log = Arc::new(FileLog::open(options(temp.path(), 512), now).unwrap());
        std::thread::scope(|scope| {
            for worker in 0..4 {
                let log = Arc::clone(&log);
                scope.spawn(move || {
                    for sequence in 0..40 {
                        log.append(&record(now, &format!("mock-{worker}-{sequence}")), now)
                            .unwrap();
                    }
                });
            }
        });
        let mut observed = BTreeSet::new();
        for path in logs(temp.path()) {
            assert!(fs::metadata(&path).unwrap().len() <= 512);
            for line in fs::read_to_string(path).unwrap().lines() {
                let value: serde_json::Value = serde_json::from_str(line).unwrap();
                assert!(observed.insert(value["fields"]["message"].as_str().unwrap().to_owned()));
            }
        }
        assert_eq!(observed.len(), 160);
    }

    #[test]
    fn competing_writers_invalid_settings_and_oversized_records_fail_visibly() {
        let temp = tempfile::tempdir().unwrap();
        let now = datetime!(2026-11-09 12:00 UTC);
        assert!(FileLog::open(options(temp.path(), 0), now).is_err());
        let config = options(temp.path(), 128);
        let log = FileLog::open(config.clone(), now).unwrap();
        assert!(FileLog::open(config, now).is_err());
        assert!(log.append(&record(now, &"x".repeat(1024)), now).is_err());
        assert_eq!(fs::metadata(temp.path().join("app.log")).unwrap().len(), 0);
        assert!(
            logs(temp.path())
                .iter()
                .all(|path| path.extension().unwrap() == "log")
        );
    }

    #[test]
    fn rotation_write_and_cleanup_failures_leave_owned_evidence_recoverable() {
        let temp = tempfile::tempdir().unwrap();
        let now = datetime!(2026-11-09 12:00 UTC);
        let bytes = record(now, "fixture");
        let log = FileLog::open(options(temp.path(), bytes.len() as u64), now).unwrap();
        log.append(&bytes, now).unwrap();
        fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o500)).unwrap();
        assert!(log.append(&bytes, now).is_err());
        assert_eq!(
            fs::metadata(temp.path().join("app.log")).unwrap().len(),
            bytes.len() as u64
        );
        fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
        log.append(&bytes, now).unwrap();
        let write_directory = temp.path().join("write-failure");
        let write_log = FileLog::open(options(&write_directory, 4096), now).unwrap();
        write_log.append(&bytes, now).unwrap();
        write_log.state.lock().unwrap().file =
            Some(File::open(write_directory.join("app.log")).unwrap());
        assert!(write_log.append(&bytes, now).is_err());
        let old = log.archive_path(now - time::Duration::days(31));
        fs::write(&old, b"old").unwrap();
        fs::set_permissions(&old, fs::Permissions::from_mode(0o600)).unwrap();
        fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o500)).unwrap();
        assert!(log.maintain(now).is_err());
        fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
        log.maintain(now).unwrap();
        assert!(!old.exists());
    }

    #[test]
    fn redaction_removes_structured_secrets_and_inline_credentials() {
        let redactor = Redactor::new();
        assert!(redactor.sensitive("authorization"));
        assert!(redactor.sensitive("private_key"));
        let input = "https://mock-user:mock-password@localhost/x?token=mock-token password=\"mock phrase\" Authorization: Bearer mock-auth";
        let output = redactor.text(input);
        for secret in [
            "mock-user",
            "mock-password",
            "mock-token",
            "mock phrase",
            "mock-auth",
        ] {
            assert!(!output.contains(secret), "{output}");
        }
        assert!(output.contains("[REDACTED]"));
    }

    #[test]
    fn interrupted_rotation_and_closed_writer_reopen_without_two_active_files() {
        let temp = tempfile::tempdir().unwrap();
        let now = datetime!(2026-11-09 12:00 UTC);
        let config = options(temp.path(), 4096);
        let log = FileLog::open(config.clone(), now).unwrap();
        log.append(&record(now, "old"), now).unwrap();
        let archive = log.archive_path(now);
        log.close().unwrap();
        assert!(log.append(&record(now, "closed"), now).is_err());
        fs::rename(temp.path().join("app.log"), archive).unwrap();
        let reopened = FileLog::open(config, now).unwrap();
        reopened.append(&record(now, "new"), now).unwrap();
        assert_eq!(logs(temp.path()).len(), 2);
        assert!(
            fs::read_to_string(temp.path().join("app.log"))
                .unwrap()
                .contains("\"new\"")
        );
    }

    #[test]
    fn unsafe_live_paths_and_corrupt_tails_are_never_adopted() {
        let temp = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let now = datetime!(2026-11-09 12:00 UTC);
        let config = options(temp.path(), 4096);
        let log = FileLog::open(config.clone(), now).unwrap();
        log.close().unwrap();
        fs::write(temp.path().join("app.log"), b"partial synthetic event").unwrap();
        assert!(FileLog::open(config.clone(), now).is_err());
        fs::remove_file(temp.path().join("app.log")).unwrap();
        let external = outside.path().join("external.log");
        fs::write(&external, b"preserve").unwrap();
        std::os::unix::fs::symlink(&external, temp.path().join("app.log")).unwrap();
        assert!(FileLog::open(config, now).is_err());
        assert_eq!(fs::read(external).unwrap(), b"preserve");
    }

    #[test]
    fn event_and_span_redaction_happens_before_either_sink_is_serialized() {
        let temp = tempfile::tempdir().unwrap();
        let log = Arc::new(
            FileLog::open(options(temp.path(), 65536), OffsetDateTime::now_utc()).unwrap(),
        );
        let subscriber = tracing_subscriber::registry().with(LoggingLayer {
            file: Some(Arc::clone(&log)),
            redactor: Redactor::new(),
        });
        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!("mock", auth_token = "mock-span-secret");
            let _entered = span.enter();
            tracing::info!(
                password = "mock-password",
                private_key = "mock-key",
                message = "synthetic"
            );
        });
        let text = fs::read_to_string(temp.path().join("app.log")).unwrap();
        for secret in ["mock-span-secret", "mock-password", "mock-key"] {
            assert!(!text.contains(secret));
        }
        assert!(text.contains("[REDACTED]") && text.contains("synthetic"));
    }

    #[test]
    fn relative_settings_use_node_base_and_never_initialize_a_missing_node() {
        let temp = tempfile::tempdir().unwrap();
        let node = temp.path().join("missing");
        let settings = Options {
            log_dir: Some(PathBuf::from("custom")),
            log_max_bytes: 4096,
            log_retention_days: 7,
            no_file_log: false,
        };
        assert!(settings.file_options(&node).is_err());
        assert!(!node.exists());
        fs::create_dir(&node).unwrap();
        fs::write(node.join("node.db"), b"synthetic path fixture").unwrap();
        assert_eq!(
            settings.file_options(&node).unwrap().unwrap().directory,
            node.canonicalize().unwrap().join("custom")
        );
    }
}
