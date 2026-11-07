// Transaction validation and KV semantics follow etcd v3.7.3 (Apache-2.0).
// See proto/etcd/LICENSE for the upstream license.
use super::{Apply, KvReply, KvRequest, MAX_REQUEST_BYTES, MAX_TXN_OPS, Metadata, digest, header};
use crate::{
    error::{Result, RpcError},
    wire::{etcdserverpb as pb, mvccpb},
};
use prost::Message;
use rusqlite::{Connection, OptionalExtension, params};
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
};
use tonic::Code;

type HistoryRow = (i64, i64, Vec<u8>, i32, Vec<u8>, Option<Vec<u8>>, Vec<u8>);
type KeyRange = (Vec<u8>, Vec<u8>);
type WriteIntervals = (BTreeSet<Vec<u8>>, Vec<KeyRange>);

impl Apply<'_> {
    pub(super) fn execute(&mut self, request: &KvRequest) -> Result<KvReply> {
        validate(request)?;
        match request {
            KvRequest::Range(request) => Ok(KvReply::Range(range(
                self.transaction,
                &self.metadata,
                request,
            )?)),
            KvRequest::Put(request) => {
                check_put(self.transaction, request)?;
                Ok(KvReply::Put(self.put(request)?))
            }
            KvRequest::Delete(request) => Ok(KvReply::Delete(self.delete(request)?)),
            KvRequest::Txn(request) => {
                let mut path = Vec::new();
                select_path(self.transaction, &self.metadata, request, &mut path)?;
                check_path(self.transaction, &self.metadata, request, &path, &mut 0)?;
                let mut response = self.txn(request, &path, &mut 0)?;
                response.header = header(self.metadata.revision);
                Ok(KvReply::Txn(response))
            }
            KvRequest::Compact(request) => Ok(KvReply::Compact(self.compact(request.revision)?)),
        }
    }

    fn modify(&mut self) -> Result<i64> {
        if !self.modified {
            self.metadata.revision = self.metadata.revision.checked_add(1).ok_or_else(|| {
                RpcError::new(Code::OutOfRange, "etcdserver: MVCC revision exhausted")
            })?;
            self.modified = true;
        }
        Ok(self.metadata.revision)
    }

    fn put(&mut self, request: &pb::PutRequest) -> Result<pb::PutResponse> {
        let previous = current(self.transaction, &request.key)?;
        let old_size = previous.as_ref().map_or(0, Message::encoded_len);
        let revision = self.modify()?;
        let value = if request.ignore_value {
            previous.as_ref().ok_or_else(key_not_found)?.value.clone()
        } else {
            request.value.clone()
        };
        let lease = if request.ignore_lease {
            previous.as_ref().ok_or_else(key_not_found)?.lease
        } else {
            request.lease
        };
        let version = previous.as_ref().map_or(Ok(1), |kv| {
            kv.version
                .checked_add(1)
                .ok_or_else(|| RpcError::new(Code::OutOfRange, "etcdserver: key version exhausted"))
        })?;
        let kv = mvccpb::KeyValue {
            key: request.key.clone(),
            value,
            version,
            lease,
            create_revision: previous.as_ref().map_or(revision, |kv| kv.create_revision),
            mod_revision: revision,
        };
        let bytes = kv.encode_to_vec();
        let added = bytes.len() + previous.as_ref().map_or(0, Message::encoded_len) + 128;
        quota(&self.metadata, bytes.len() + added, old_size)?;
        self.metadata.logical_bytes = self
            .metadata
            .logical_bytes
            .checked_sub(old_size as u64)
            .and_then(|size| size.checked_add((bytes.len() + added) as u64))
            .ok_or_else(|| RpcError::corrupt("logical storage accounting overflow"))?;
        self.transaction
            .execute("DELETE FROM lease_keys WHERE key = ?1", [&request.key])?;
        self.transaction.execute(
            "INSERT INTO current_kvs(key, kv, create_revision, mod_revision, version, lease, checksum)
             VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(key) DO UPDATE SET kv = excluded.kv, create_revision = excluded.create_revision,
             mod_revision = excluded.mod_revision, version = excluded.version, lease = excluded.lease, checksum = excluded.checksum",
            params![kv.key, bytes, kv.create_revision, kv.mod_revision, kv.version,
                if lease == 0 { None } else { Some(lease) }, digest(&[&kv.key, &bytes])],
        )?;
        if lease != 0 {
            self.transaction.execute(
                "INSERT INTO lease_keys(lease, key) VALUES(?1, ?2)",
                params![lease, kv.key],
            )?;
        }
        self.history(&kv.key, 0, &bytes, previous.as_ref())?;
        Ok(pb::PutResponse {
            header: header(revision),
            prev_kv: if request.prev_kv { previous } else { None },
        })
    }

    pub(super) fn delete(
        &mut self,
        request: &pb::DeleteRangeRequest,
    ) -> Result<pb::DeleteRangeResponse> {
        let keys = visible(
            self.transaction,
            &self.metadata,
            &request.key,
            &request.range_end,
            0,
        )?;
        if keys.is_empty() {
            return Ok(pb::DeleteRangeResponse {
                header: header(self.metadata.revision),
                ..Default::default()
            });
        }
        let revision = self.modify()?;
        for previous in &keys {
            let tombstone = mvccpb::KeyValue {
                key: previous.key.clone(),
                ..Default::default()
            }
            .encode_to_vec();
            self.transaction
                .execute("DELETE FROM current_kvs WHERE key = ?1", [&previous.key])?;
            self.history(&previous.key, 1, &tombstone, Some(previous))?;
            self.metadata.logical_bytes = self
                .metadata
                .logical_bytes
                .checked_add((tombstone.len() + 128) as u64)
                .ok_or_else(|| RpcError::corrupt("logical storage accounting overflow"))?;
        }
        Ok(pb::DeleteRangeResponse {
            header: header(revision),
            deleted: count(keys.len())?,
            prev_kvs: if request.prev_kv { keys } else { Vec::new() },
        })
    }

    fn history(
        &mut self,
        key: &[u8],
        kind: i32,
        kv: &[u8],
        previous: Option<&mvccpb::KeyValue>,
    ) -> Result<()> {
        let previous = previous.map(Message::encode_to_vec);
        let checksum = history_digest(
            self.metadata.revision,
            self.sub_revision,
            key,
            kind,
            kv,
            previous.as_deref(),
        );
        self.transaction.execute(
            "INSERT INTO mvcc_versions(main_revision, sub_revision, key, kind, kv, prev_kv, checksum)
             VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![self.metadata.revision, self.sub_revision, key, kind, kv, previous, checksum],
        )?;
        self.sub_revision = self.sub_revision.checked_add(1).ok_or_else(|| {
            RpcError::new(Code::OutOfRange, "etcdserver: MVCC subrevision exhausted")
        })?;
        Ok(())
    }

    fn txn(
        &mut self,
        request: &pb::TxnRequest,
        path: &[bool],
        cursor: &mut usize,
    ) -> Result<pb::TxnResponse> {
        let succeeded = path[*cursor];
        *cursor += 1;
        let operations = if succeeded {
            &request.success
        } else {
            &request.failure
        };
        let mut responses = Vec::with_capacity(operations.len());
        for operation in operations {
            let response = match operation.request.as_ref().ok_or_else(key_not_found)? {
                pb::request_op::Request::RequestRange(request) => {
                    pb::response_op::Response::ResponseRange(range(
                        self.transaction,
                        &self.metadata,
                        request,
                    )?)
                }
                pb::request_op::Request::RequestPut(request) => {
                    pb::response_op::Response::ResponsePut(self.put(request)?)
                }
                pb::request_op::Request::RequestDeleteRange(request) => {
                    pb::response_op::Response::ResponseDeleteRange(self.delete(request)?)
                }
                pb::request_op::Request::RequestTxn(request) => {
                    pb::response_op::Response::ResponseTxn(self.txn(request, path, cursor)?)
                }
            };
            responses.push(pb::ResponseOp {
                response: Some(response),
            });
        }
        Ok(pb::TxnResponse {
            header: header(0),
            succeeded,
            responses,
        })
    }

    fn compact(&mut self, revision: i64) -> Result<pb::CompactionResponse> {
        if revision <= self.metadata.compacted_revision {
            return Err(compacted());
        }
        if revision > self.metadata.revision {
            return Err(future());
        }
        if revision < 0 {
            return Err(compacted());
        }
        let mut statement = self.transaction.prepare(
            "SELECT main_revision, sub_revision, kind FROM (
             SELECT main_revision, sub_revision, kind,
                    ROW_NUMBER() OVER(PARTITION BY key ORDER BY main_revision DESC, sub_revision DESC) AS latest
             FROM mvcc_versions WHERE main_revision <= ?1) WHERE latest = 1",
        )?;
        let rows = statement
            .query_map([revision], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i32>(2)?,
                ))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        drop(statement);
        self.transaction.execute_batch("CREATE TEMP TABLE IF NOT EXISTS compact_keep(main INTEGER, sub INTEGER, PRIMARY KEY(main, sub)); DELETE FROM compact_keep;")?;
        for (main, sub, kind) in rows {
            if kind == 0 || main == revision {
                self.transaction.execute(
                    "INSERT INTO compact_keep(main, sub) VALUES(?1, ?2)",
                    params![main, sub],
                )?;
            }
        }
        self.transaction.execute(
            "DELETE FROM mvcc_versions WHERE main_revision <= ?1 AND
             NOT EXISTS(SELECT 1 FROM compact_keep WHERE main = main_revision AND sub = sub_revision)", [revision],
        )?;
        self.transaction.execute_batch("DELETE FROM compact_keep")?;
        self.metadata.compacted_revision = revision;
        let bytes: i64 = self.transaction.query_row(
            "SELECT COALESCE((SELECT SUM(length(kv)) FROM current_kvs), 0) +
             COALESCE((SELECT SUM(length(kv) + COALESCE(length(prev_kv), 0) + 128) FROM mvcc_versions), 0)",
            [], |row| row.get(0),
        )?;
        self.metadata.logical_bytes = u64::try_from(bytes)
            .map_err(|_| RpcError::corrupt("negative logical storage accounting"))?;
        Ok(pb::CompactionResponse {
            header: header(self.metadata.revision),
        })
    }
}

pub(crate) fn validate(request: &KvRequest) -> Result<()> {
    let length = match request {
        KvRequest::Range(request) => request.encoded_len(),
        KvRequest::Put(request) => request.encoded_len(),
        KvRequest::Delete(request) => request.encoded_len(),
        KvRequest::Txn(request) => request.encoded_len(),
        KvRequest::Compact(request) => request.encoded_len(),
    };
    if length > MAX_REQUEST_BYTES {
        return Err(RpcError::invalid("etcdserver: request is too large"));
    }
    match request {
        KvRequest::Range(request) => validate_range(request),
        KvRequest::Put(request) => validate_put(request),
        KvRequest::Delete(request) => key(&request.key),
        KvRequest::Txn(request) => {
            validate_txn(request, MAX_TXN_OPS)?;
            intervals(&request.success)?;
            intervals(&request.failure)?;
            Ok(())
        }
        KvRequest::Compact(_) => Ok(()),
    }
}

pub(crate) fn validate_range(request: &pb::RangeRequest) -> Result<()> {
    key(&request.key)?;
    if pb::range_request::SortOrder::try_from(request.sort_order).is_err()
        || pb::range_request::SortTarget::try_from(request.sort_target).is_err()
    {
        return Err(RpcError::invalid("etcdserver: invalid sort option"));
    }
    Ok(())
}

fn validate_put(request: &pb::PutRequest) -> Result<()> {
    key(&request.key)?;
    if request.ignore_value && !request.value.is_empty() {
        return Err(RpcError::invalid("etcdserver: value is provided"));
    }
    if request.ignore_lease && request.lease != 0 {
        return Err(RpcError::invalid("etcdserver: lease is provided"));
    }
    Ok(())
}

fn validate_txn(request: &pb::TxnRequest, budget: usize) -> Result<()> {
    let operations = request
        .compare
        .len()
        .max(request.success.len())
        .max(request.failure.len());
    if operations > budget {
        return Err(RpcError::invalid(
            "etcdserver: too many operations in txn request",
        ));
    }
    for compare in &request.compare {
        key(&compare.key)?;
    }
    for operation in request.success.iter().chain(&request.failure) {
        match operation.request.as_ref().ok_or_else(key_not_found)? {
            pb::request_op::Request::RequestRange(request) => validate_range(request)?,
            pb::request_op::Request::RequestPut(request) => validate_put(request)?,
            pb::request_op::Request::RequestDeleteRange(request) => key(&request.key)?,
            pb::request_op::Request::RequestTxn(request) => {
                validate_txn(request, budget - operations)?
            }
        }
    }
    Ok(())
}

fn intervals(operations: &[pb::RequestOp]) -> Result<WriteIntervals> {
    let mut deletes = Vec::new();
    for operation in operations {
        if let Some(pb::request_op::Request::RequestDeleteRange(request)) = &operation.request {
            deletes.push((request.key.clone(), request.range_end.clone()));
        }
    }
    let mut puts = BTreeSet::new();
    for operation in operations {
        if let Some(pb::request_op::Request::RequestTxn(request)) = &operation.request {
            let (then_puts, then_deletes) = intervals(&request.success)?;
            let (else_puts, else_deletes) = intervals(&request.failure)?;
            for value in &then_puts {
                if puts.contains(value)
                    || deletes
                        .iter()
                        .any(|(start, end)| contains(start, end, value))
                {
                    return Err(duplicate());
                }
                puts.insert(value.clone());
            }
            for value in &else_puts {
                if (puts.contains(value) && !then_puts.contains(value))
                    || deletes
                        .iter()
                        .any(|(start, end)| contains(start, end, value))
                {
                    return Err(duplicate());
                }
                puts.insert(value.clone());
            }
            deletes.extend(then_deletes);
            deletes.extend(else_deletes);
        }
    }
    for operation in operations {
        if let Some(pb::request_op::Request::RequestPut(request)) = &operation.request
            && (!puts.insert(request.key.clone())
                || deletes
                    .iter()
                    .any(|(start, end)| contains(start, end, &request.key)))
        {
            return Err(duplicate());
        }
    }
    Ok((puts, deletes))
}

fn select_path(
    connection: &Connection,
    metadata: &Metadata,
    request: &pb::TxnRequest,
    path: &mut Vec<bool>,
) -> Result<()> {
    let mut succeeded = true;
    for compare in &request.compare {
        let values = visible(connection, metadata, &compare.key, &compare.range_end, 0)?;
        if values.is_empty() {
            if compare.target == pb::compare::CompareTarget::Value as i32
                || !compare_value(compare, &mvccpb::KeyValue::default())
            {
                succeeded = false;
                break;
            }
        } else if !values.iter().all(|kv| compare_value(compare, kv)) {
            succeeded = false;
            break;
        }
    }
    path.push(succeeded);
    for operation in if succeeded {
        &request.success
    } else {
        &request.failure
    } {
        if let Some(pb::request_op::Request::RequestTxn(request)) = &operation.request {
            select_path(connection, metadata, request, path)?;
        }
    }
    Ok(())
}

fn compare_value(compare: &pb::Compare, kv: &mvccpb::KeyValue) -> bool {
    use pb::compare::{CompareResult, CompareTarget, TargetUnion};
    let order = match CompareTarget::try_from(compare.target) {
        Ok(CompareTarget::Value) => {
            let value = match &compare.target_union {
                Some(TargetUnion::Value(value)) => value.as_slice(),
                _ => &[],
            };
            kv.value.as_slice().cmp(value)
        }
        Ok(CompareTarget::Version) => kv.version.cmp(&match compare.target_union {
            Some(TargetUnion::Version(value)) => value,
            _ => 0,
        }),
        Ok(CompareTarget::Create) => kv.create_revision.cmp(&match compare.target_union {
            Some(TargetUnion::CreateRevision(value)) => value,
            _ => 0,
        }),
        Ok(CompareTarget::Mod) => kv.mod_revision.cmp(&match compare.target_union {
            Some(TargetUnion::ModRevision(value)) => value,
            _ => 0,
        }),
        Ok(CompareTarget::Lease) => kv.lease.cmp(&match compare.target_union {
            Some(TargetUnion::Lease(value)) => value,
            _ => 0,
        }),
        Err(_) => Ordering::Equal,
    };
    match CompareResult::try_from(compare.result) {
        Ok(CompareResult::Equal) => order == Ordering::Equal,
        Ok(CompareResult::NotEqual) => order != Ordering::Equal,
        Ok(CompareResult::Greater) => order == Ordering::Greater,
        Ok(CompareResult::Less) => order == Ordering::Less,
        Err(_) => true,
    }
}

fn check_path(
    connection: &Connection,
    metadata: &Metadata,
    request: &pb::TxnRequest,
    path: &[bool],
    cursor: &mut usize,
) -> Result<()> {
    let succeeded = path[*cursor];
    *cursor += 1;
    for operation in if succeeded {
        &request.success
    } else {
        &request.failure
    } {
        match operation.request.as_ref().ok_or_else(key_not_found)? {
            pb::request_op::Request::RequestRange(request) => {
                if request.revision != 0 {
                    if request.revision > metadata.revision {
                        return Err(future());
                    }
                    if request.revision <= metadata.compacted_revision {
                        return Err(compacted());
                    }
                }
            }
            pb::request_op::Request::RequestPut(request) => check_put(connection, request)?,
            pb::request_op::Request::RequestDeleteRange(_) => {}
            pb::request_op::Request::RequestTxn(request) => {
                check_path(connection, metadata, request, path, cursor)?
            }
        }
    }
    Ok(())
}

fn check_put(connection: &Connection, request: &pb::PutRequest) -> Result<()> {
    if (request.ignore_value || request.ignore_lease)
        && current(connection, &request.key)?.is_none()
    {
        return Err(key_not_found());
    }
    if request.lease != 0 && !request.ignore_lease {
        let exists: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM leases WHERE id = ?1)",
            [request.lease],
            |row| row.get(0),
        )?;
        if !exists {
            return Err(RpcError::new(
                Code::NotFound,
                "etcdserver: requested lease not found",
            ));
        }
    }
    Ok(())
}

pub(super) fn current(connection: &Connection, key: &[u8]) -> Result<Option<mvccpb::KeyValue>> {
    let row: Option<(Vec<u8>, Vec<u8>)> = connection
        .query_row(
            "SELECT kv, checksum FROM current_kvs WHERE key = ?1",
            [key],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    row.map(|(bytes, checksum)| decode_current(key, &bytes, &checksum))
        .transpose()
}

fn decode_current(key: &[u8], bytes: &[u8], checksum: &[u8]) -> Result<mvccpb::KeyValue> {
    if digest(&[key, bytes]) != checksum {
        return Err(RpcError::corrupt("current KV checksum mismatch"));
    }
    let kv = mvccpb::KeyValue::decode(bytes)?;
    if kv.key != key
        || kv.create_revision < 1
        || kv.mod_revision < kv.create_revision
        || kv.version < 1
    {
        return Err(RpcError::corrupt(
            "current KV fields violate MVCC invariants",
        ));
    }
    Ok(kv)
}

fn visible(
    connection: &Connection,
    metadata: &Metadata,
    key: &[u8],
    end: &[u8],
    revision: i64,
) -> Result<Vec<mvccpb::KeyValue>> {
    if revision > metadata.revision {
        return Err(future());
    }
    if revision > 0 && revision <= metadata.compacted_revision {
        return Err(compacted());
    }
    let condition = if end.is_empty() {
        "key = ?1"
    } else if end == [0] {
        "key >= ?1"
    } else {
        "key >= ?1 AND key < ?2"
    };
    let historical = revision > 0 && revision != metadata.revision;
    let sql = if historical {
        format!("SELECT main_revision, sub_revision, key, kind, kv, prev_kv, checksum FROM (
            SELECT main_revision, sub_revision, key, kind, kv, prev_kv, checksum,
                   ROW_NUMBER() OVER(PARTITION BY key ORDER BY main_revision DESC, sub_revision DESC) AS latest
            FROM mvcc_versions WHERE {condition} AND main_revision <= ?3) WHERE latest = 1 ORDER BY key")
    } else {
        format!("SELECT key, kv, checksum FROM current_kvs WHERE {condition} ORDER BY key")
    };
    let mut statement = connection.prepare(&sql)?;
    let mut rows = if historical {
        statement.query(params![key, end, revision])?
    } else if end.is_empty() || end == [0] {
        statement.query([key])?
    } else {
        statement.query(params![key, end])?
    };
    let mut values = Vec::new();
    while let Some(row) = rows.next()? {
        let kv = if historical {
            let history: HistoryRow = (
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
            );
            validate_history(&history)?;
            if history.3 == 1 {
                continue;
            }
            mvccpb::KeyValue::decode(history.4.as_slice())?
        } else {
            let key: Vec<u8> = row.get(0)?;
            let bytes: Vec<u8> = row.get(1)?;
            let checksum: Vec<u8> = row.get(2)?;
            decode_current(&key, &bytes, &checksum)?
        };
        values.push(kv);
    }
    Ok(values)
}

pub(super) fn range(
    connection: &Connection,
    metadata: &Metadata,
    request: &pb::RangeRequest,
) -> Result<pb::RangeResponse> {
    let mut values = visible(
        connection,
        metadata,
        &request.key,
        &request.range_end,
        request.revision,
    )?;
    let total = count(values.len())?;
    if request.count_only {
        return Ok(pb::RangeResponse {
            header: header(metadata.revision),
            count: total,
            ..Default::default()
        });
    }
    let has_filters = request.min_mod_revision != 0
        || request.max_mod_revision != 0
        || request.min_create_revision != 0
        || request.max_create_revision != 0;
    let default_order =
        request.sort_order == 0 || (request.sort_order == 1 && request.sort_target == 0);
    if default_order && !has_filters && request.limit > 0 && request.limit < i64::MAX {
        values.truncate(
            usize::try_from(request.limit + 1)
                .map_err(|_| RpcError::invalid("range limit exceeds platform bounds"))?,
        );
    }
    values.retain(|kv| {
        (request.min_mod_revision == 0 || kv.mod_revision >= request.min_mod_revision)
            && (request.max_mod_revision == 0 || kv.mod_revision <= request.max_mod_revision)
            && (request.min_create_revision == 0
                || kv.create_revision >= request.min_create_revision)
            && (request.max_create_revision == 0
                || kv.create_revision <= request.max_create_revision)
    });
    let order = if request.sort_target != 0 && request.sort_order == 0 {
        1
    } else {
        request.sort_order
    };
    if order != 0 {
        values.sort_by(|left, right| {
            let ordering = match pb::range_request::SortTarget::try_from(request.sort_target) {
                Ok(pb::range_request::SortTarget::Key) => left.key.cmp(&right.key),
                Ok(pb::range_request::SortTarget::Version) => left.version.cmp(&right.version),
                Ok(pb::range_request::SortTarget::Create) => {
                    left.create_revision.cmp(&right.create_revision)
                }
                Ok(pb::range_request::SortTarget::Mod) => {
                    left.mod_revision.cmp(&right.mod_revision)
                }
                Ok(pb::range_request::SortTarget::Value) => left.value.cmp(&right.value),
                Err(_) => Ordering::Equal,
            };
            if order == 2 {
                ordering.reverse()
            } else {
                ordering
            }
        });
    }
    let more = request.limit > 0 && count(values.len())? > request.limit;
    if more {
        values.truncate(
            usize::try_from(request.limit)
                .map_err(|_| RpcError::invalid("range limit exceeds platform bounds"))?,
        );
    }
    if request.keys_only {
        for kv in &mut values {
            kv.value.clear();
        }
    }
    Ok(pb::RangeResponse {
        header: header(metadata.revision),
        kvs: values,
        more,
        count: total,
    })
}

pub(super) fn events(
    connection: &Connection,
    start: i64,
    key: &[u8],
    end: &[u8],
) -> Result<Vec<(i64, Vec<mvccpb::Event>)>> {
    let mut statement = connection.prepare(
        "SELECT main_revision, sub_revision, key, kind, kv, prev_kv, checksum
         FROM mvcc_versions WHERE main_revision >= ?1 ORDER BY main_revision, sub_revision",
    )?;
    let mut rows = statement.query([start])?;
    let mut batches = BTreeMap::<i64, Vec<mvccpb::Event>>::new();
    while let Some(row) = rows.next()? {
        let history: HistoryRow = (
            row.get(0)?,
            row.get(1)?,
            row.get(2)?,
            row.get(3)?,
            row.get(4)?,
            row.get(5)?,
            row.get(6)?,
        );
        if !contains(key, end, &history.2) {
            continue;
        }
        validate_history(&history)?;
        let mut kv = mvccpb::KeyValue::decode(history.4.as_slice())?;
        if history.3 == 1 {
            kv.mod_revision = history.0;
        }
        let previous = history
            .5
            .map(|bytes| mvccpb::KeyValue::decode(bytes.as_slice()))
            .transpose()?;
        batches.entry(history.0).or_default().push(mvccpb::Event {
            r#type: history.3,
            kv: Some(kv),
            prev_kv: previous,
        });
    }
    Ok(batches.into_iter().collect())
}

fn validate_history(history: &HistoryRow) -> Result<()> {
    if history_digest(
        history.0,
        history.1,
        &history.2,
        history.3,
        &history.4,
        history.5.as_deref(),
    ) != history.6
    {
        return Err(RpcError::corrupt("MVCC history checksum mismatch"));
    }
    Ok(())
}

pub(crate) fn history_digest(
    main: i64,
    sub: i64,
    key: &[u8],
    kind: i32,
    kv: &[u8],
    previous: Option<&[u8]>,
) -> Vec<u8> {
    digest(&[
        &main.to_be_bytes(),
        &sub.to_be_bytes(),
        key,
        &kind.to_be_bytes(),
        kv,
        previous.unwrap_or(&[]),
    ])
}

pub(crate) fn contains(start: &[u8], end: &[u8], key: &[u8]) -> bool {
    if end.is_empty() {
        key == start
    } else {
        key >= start && (end == [0] || key < end)
    }
}

fn quota(metadata: &Metadata, added: usize, removed: usize) -> Result<()> {
    let size = metadata
        .logical_bytes
        .checked_sub(removed as u64)
        .and_then(|size| size.checked_add(added as u64))
        .ok_or_else(|| RpcError::corrupt("logical storage accounting overflow"))?;
    if size > metadata.quota_bytes {
        return Err(RpcError::new(
            Code::ResourceExhausted,
            "etcdserver: mvcc: database space exceeded",
        ));
    }
    Ok(())
}

fn key(key: &[u8]) -> Result<()> {
    if key.is_empty() {
        Err(RpcError::invalid("etcdserver: key is not provided"))
    } else {
        Ok(())
    }
}
fn key_not_found() -> RpcError {
    RpcError::invalid("etcdserver: key not found")
}
fn duplicate() -> RpcError {
    RpcError::invalid("etcdserver: duplicate key given in txn request")
}
fn compacted() -> RpcError {
    RpcError::new(
        Code::OutOfRange,
        "etcdserver: mvcc: required revision has been compacted",
    )
}
fn future() -> RpcError {
    RpcError::new(
        Code::OutOfRange,
        "etcdserver: mvcc: required revision is a future revision",
    )
}
fn count(length: usize) -> Result<i64> {
    i64::try_from(length)
        .map_err(|_| RpcError::corrupt("result count exceeds signed 64-bit bounds"))
}
