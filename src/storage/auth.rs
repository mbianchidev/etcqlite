// Range authorization and user/role behavior follow etcd v3.7.3 (Apache-2.0).
// See proto/etcd/LICENSE for the upstream license.
use super::{Apply, KvRequest, Metadata, Store, digest, header, lease, mvcc};
use crate::{
    error::{Result, RpcError},
    wire::{authpb, etcdserverpb as pb},
};
use prost::Message;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use tonic::Code;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Principal {
    Anonymous,
    User { name: String, revision: u64 },
    Internal,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum AuthCommand {
    Enable,
    Disable,
    UserAdd {
        name: String,
        #[serde(with = "crate::bytes")]
        password_hash: Vec<u8>,
        options: authpb::UserAddOptions,
    },
    UserDelete {
        name: String,
    },
    UserChangePassword {
        name: String,
        #[serde(with = "crate::bytes")]
        password_hash: Vec<u8>,
    },
    UserGrantRole {
        user: String,
        role: String,
    },
    UserRevokeRole {
        user: String,
        role: String,
    },
    RoleAdd {
        name: String,
    },
    RoleDelete {
        name: String,
    },
    RoleGrantPermission {
        name: String,
        permission: Option<authpb::Permission>,
    },
    RoleRevokePermission {
        name: String,
        key: Vec<u8>,
        range_end: Vec<u8>,
    },
    Authenticate {
        name: String,
        #[serde(with = "crate::bytes")]
        expected_password_hash: Vec<u8>,
        expected_auth_revision: u64,
        token_hash: [u8; 32],
        expires_at_ms: i64,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum AuthQuery {
    Status,
    UserGet { name: String },
    UserList,
    RoleGet { name: String },
    RoleList,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum AuthReply {
    Enabled(pb::AuthEnableResponse),
    Disabled(pb::AuthDisableResponse),
    Status(pb::AuthStatusResponse),
    UserAdded(pb::AuthUserAddResponse),
    UserDeleted(pb::AuthUserDeleteResponse),
    PasswordChanged(pb::AuthUserChangePasswordResponse),
    UserRoleGranted(pb::AuthUserGrantRoleResponse),
    UserRoleRevoked(pb::AuthUserRevokeRoleResponse),
    RoleAdded(pb::AuthRoleAddResponse),
    RoleDeleted(pb::AuthRoleDeleteResponse),
    PermissionGranted(pb::AuthRoleGrantPermissionResponse),
    PermissionRevoked(pb::AuthRoleRevokePermissionResponse),
    UserGet(pb::AuthUserGetResponse),
    UserList(pb::AuthUserListResponse),
    RoleGet(pb::AuthRoleGetResponse),
    RoleList(pb::AuthRoleListResponse),
    Authenticated {
        header: Option<pb::ResponseHeader>,
        token_hash: [u8; 32],
    },
}

impl AuthReply {
    pub fn header_mut(&mut self) -> Option<&mut pb::ResponseHeader> {
        match self {
            Self::Enabled(reply) => reply.header.as_mut(),
            Self::Disabled(reply) => reply.header.as_mut(),
            Self::Status(reply) => reply.header.as_mut(),
            Self::UserAdded(reply) => reply.header.as_mut(),
            Self::UserDeleted(reply) => reply.header.as_mut(),
            Self::PasswordChanged(reply) => reply.header.as_mut(),
            Self::UserRoleGranted(reply) => reply.header.as_mut(),
            Self::UserRoleRevoked(reply) => reply.header.as_mut(),
            Self::RoleAdded(reply) => reply.header.as_mut(),
            Self::RoleDeleted(reply) => reply.header.as_mut(),
            Self::PermissionGranted(reply) => reply.header.as_mut(),
            Self::PermissionRevoked(reply) => reply.header.as_mut(),
            Self::UserGet(reply) => reply.header.as_mut(),
            Self::UserList(reply) => reply.header.as_mut(),
            Self::RoleGet(reply) => reply.header.as_mut(),
            Self::RoleList(reply) => reply.header.as_mut(),
            Self::Authenticated { header, .. } => header.as_mut(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Credentials {
    pub password_hash: Vec<u8>,
    pub revision: u64,
    pub no_password: bool,
}

impl Apply<'_> {
    pub(super) fn execute_auth(&mut self, command: &AuthCommand) -> Result<AuthReply> {
        if !matches!(command, AuthCommand::Authenticate { .. }) {
            require_admin(self.transaction, &self.metadata, &self.principal)?;
        }
        let response_header = || header(self.metadata.revision);
        match command {
            AuthCommand::Enable => {
                if !self.metadata.auth_enabled {
                    let root = user(self.transaction, "root")?.ok_or_else(|| {
                        RpcError::new(
                            Code::FailedPrecondition,
                            "etcdserver: root user does not exist",
                        )
                    })?;
                    if !root.roles.iter().any(|role| role == "root") {
                        return Err(RpcError::new(
                            Code::FailedPrecondition,
                            "etcdserver: root user does not have root role",
                        ));
                    }
                    self.metadata.auth_enabled = true;
                }
                Ok(AuthReply::Enabled(pb::AuthEnableResponse {
                    header: response_header(),
                }))
            }
            AuthCommand::Disable => {
                if self.metadata.auth_enabled {
                    self.metadata.auth_enabled = false;
                    self.bump_auth()?;
                    self.transaction.execute("DELETE FROM auth_tokens", [])?;
                }
                Ok(AuthReply::Disabled(pb::AuthDisableResponse {
                    header: header(self.metadata.revision),
                }))
            }
            AuthCommand::UserAdd {
                name,
                password_hash,
                options,
            } => {
                if name.is_empty() {
                    return Err(RpcError::invalid("etcdserver: user name is empty"));
                }
                if user(self.transaction, name)?.is_some() {
                    return Err(RpcError::new(
                        Code::FailedPrecondition,
                        "etcdserver: user name already exists",
                    ));
                }
                if !options.no_password && password_hash.is_empty() {
                    return Err(auth_failed());
                }
                let new_user = authpb::User {
                    name: name.as_bytes().to_vec(),
                    password: if options.no_password {
                        Vec::new()
                    } else {
                        password_hash.clone()
                    },
                    roles: Vec::new(),
                    options: Some(*options),
                };
                save_user(self.transaction, name, &new_user)?;
                self.bump_auth()?;
                Ok(AuthReply::UserAdded(pb::AuthUserAddResponse {
                    header: header(self.metadata.revision),
                }))
            }
            AuthCommand::UserDelete { name } => {
                protect(self.metadata.auth_enabled && name == "root")?;
                user(self.transaction, name)?.ok_or_else(user_not_found)?;
                self.transaction
                    .execute("DELETE FROM auth_users WHERE name = ?1", [name])?;
                self.bump_auth()?;
                Ok(AuthReply::UserDeleted(pb::AuthUserDeleteResponse {
                    header: header(self.metadata.revision),
                }))
            }
            AuthCommand::UserChangePassword {
                name,
                password_hash,
            } => {
                let mut existing = user(self.transaction, name)?.ok_or_else(user_not_found)?;
                if !existing.options.is_some_and(|options| options.no_password) {
                    if password_hash.is_empty() {
                        return Err(auth_failed());
                    }
                    existing.password = password_hash.clone();
                }
                save_user(self.transaction, name, &existing)?;
                self.transaction
                    .execute("DELETE FROM auth_tokens WHERE user = ?1", [name])?;
                self.bump_auth()?;
                Ok(AuthReply::PasswordChanged(
                    pb::AuthUserChangePasswordResponse {
                        header: header(self.metadata.revision),
                    },
                ))
            }
            AuthCommand::UserGrantRole {
                user: name,
                role: role_name,
            } => {
                let mut existing = user(self.transaction, name)?.ok_or_else(user_not_found)?;
                if role_name != "root" {
                    role(self.transaction, role_name)?.ok_or_else(role_not_found)?;
                }
                if !existing.roles.contains(role_name) {
                    existing.roles.push(role_name.clone());
                    existing.roles.sort();
                    save_user(self.transaction, name, &existing)?;
                    self.bump_auth()?;
                }
                Ok(AuthReply::UserRoleGranted(pb::AuthUserGrantRoleResponse {
                    header: header(self.metadata.revision),
                }))
            }
            AuthCommand::UserRevokeRole {
                user: name,
                role: role_name,
            } => {
                protect(self.metadata.auth_enabled && name == "root" && role_name == "root")?;
                let mut existing = user(self.transaction, name)?.ok_or_else(user_not_found)?;
                let count = existing.roles.len();
                existing.roles.retain(|role| role != role_name);
                if count == existing.roles.len() {
                    return Err(RpcError::new(
                        Code::FailedPrecondition,
                        "etcdserver: role is not granted to the user",
                    ));
                }
                save_user(self.transaction, name, &existing)?;
                self.bump_auth()?;
                Ok(AuthReply::UserRoleRevoked(pb::AuthUserRevokeRoleResponse {
                    header: header(self.metadata.revision),
                }))
            }
            AuthCommand::RoleAdd { name } => {
                if name.is_empty() {
                    return Err(RpcError::invalid("etcdserver: role name is empty"));
                }
                if role(self.transaction, name)?.is_some() {
                    return Err(RpcError::new(
                        Code::FailedPrecondition,
                        "etcdserver: role name already exists",
                    ));
                }
                save_role(
                    self.transaction,
                    name,
                    &authpb::Role {
                        name: name.as_bytes().to_vec(),
                        key_permission: Vec::new(),
                    },
                )?;
                self.bump_auth()?;
                Ok(AuthReply::RoleAdded(pb::AuthRoleAddResponse {
                    header: header(self.metadata.revision),
                }))
            }
            AuthCommand::RoleDelete { name } => {
                protect(self.metadata.auth_enabled && name == "root")?;
                role(self.transaction, name)?.ok_or_else(role_not_found)?;
                self.transaction
                    .execute("DELETE FROM auth_roles WHERE name = ?1", [name])?;
                for username in names(self.transaction, "auth_users")? {
                    let mut existing =
                        user(self.transaction, &username)?.ok_or_else(user_not_found)?;
                    if existing.roles.contains(name) {
                        existing.roles.retain(|role| role != name);
                        save_user(self.transaction, &username, &existing)?;
                    }
                }
                self.bump_auth()?;
                Ok(AuthReply::RoleDeleted(pb::AuthRoleDeleteResponse {
                    header: header(self.metadata.revision),
                }))
            }
            AuthCommand::RoleGrantPermission { name, permission } => {
                let permission = permission
                    .as_ref()
                    .ok_or_else(|| RpcError::invalid("etcdserver: permission not given"))?;
                if !valid_permission(permission) {
                    return Err(RpcError::invalid("etcdserver: invalid auth management"));
                }
                let mut existing = role(self.transaction, name)?.ok_or_else(role_not_found)?;
                if let Some(current) = existing.key_permission.iter_mut().find(|current| {
                    current.key == permission.key && current.range_end == permission.range_end
                }) {
                    current.perm_type = permission.perm_type;
                } else {
                    existing.key_permission.push(permission.clone());
                    existing
                        .key_permission
                        .sort_by(|left, right| left.key.cmp(&right.key));
                }
                save_role(self.transaction, name, &existing)?;
                self.bump_auth()?;
                Ok(AuthReply::PermissionGranted(
                    pb::AuthRoleGrantPermissionResponse {
                        header: header(self.metadata.revision),
                    },
                ))
            }
            AuthCommand::RoleRevokePermission {
                name,
                key,
                range_end,
            } => {
                let mut existing = role(self.transaction, name)?.ok_or_else(role_not_found)?;
                let count = existing.key_permission.len();
                existing.key_permission.retain(|permission| {
                    &permission.key != key || &permission.range_end != range_end
                });
                if count == existing.key_permission.len() {
                    return Err(RpcError::new(
                        Code::FailedPrecondition,
                        "etcdserver: permission is not granted to the role",
                    ));
                }
                save_role(self.transaction, name, &existing)?;
                self.bump_auth()?;
                Ok(AuthReply::PermissionRevoked(
                    pb::AuthRoleRevokePermissionResponse {
                        header: header(self.metadata.revision),
                    },
                ))
            }
            AuthCommand::Authenticate {
                name,
                expected_password_hash,
                expected_auth_revision,
                token_hash,
                expires_at_ms,
            } => {
                if !self.metadata.auth_enabled {
                    return Err(RpcError::new(
                        Code::FailedPrecondition,
                        "etcdserver: authentication is not enabled",
                    ));
                }
                let existing = user(self.transaction, name)?.ok_or_else(auth_failed)?;
                if existing.options.is_some_and(|options| options.no_password)
                    || &existing.password != expected_password_hash
                    || *expected_auth_revision != self.metadata.auth_revision
                {
                    return Err(auth_failed());
                }
                self.transaction.execute(
                    "INSERT INTO auth_tokens(token_hash, user, auth_revision, expires_at_ms) VALUES(?1, ?2, ?3, ?4)",
                    params![token_hash, name, self.metadata.auth_revision.to_be_bytes(), expires_at_ms],
                )?;
                Ok(AuthReply::Authenticated {
                    header: header(self.metadata.revision),
                    token_hash: *token_hash,
                })
            }
        }
    }

    fn bump_auth(&mut self) -> Result<()> {
        self.metadata.auth_revision =
            self.metadata.auth_revision.checked_add(1).ok_or_else(|| {
                RpcError::new(Code::OutOfRange, "etcdserver: auth revision exhausted")
            })?;
        Ok(())
    }
}

impl Store {
    pub fn require_authenticated(&self, principal: &Principal) -> Result<()> {
        self.ready()?;
        self.guard(require_user(&self.connection, &self.metadata, principal).map(|_| ()))
    }
    pub fn check_admin(&self, principal: &Principal) -> Result<()> {
        self.ready()?;
        self.guard(require_admin(&self.connection, &self.metadata, principal))
    }
    pub fn credentials(&self, name: &str) -> Result<Credentials> {
        self.ready()?;
        self.guard((|| {
            if !self.metadata.auth_enabled {
                return Err(RpcError::new(
                    Code::FailedPrecondition,
                    "etcdserver: authentication is not enabled",
                ));
            }
            let existing = user(&self.connection, name)?.ok_or_else(auth_failed)?;
            Ok(Credentials {
                password_hash: existing.password,
                revision: self.metadata.auth_revision,
                no_password: existing.options.is_some_and(|options| options.no_password),
            })
        })())
    }

    pub fn principal(
        &self,
        token_hash: Option<&[u8; 32]>,
        certificate_name: Option<&str>,
        now_ms: i64,
    ) -> Result<Principal> {
        self.ready()?;
        self.guard((|| {
            if !self.metadata.auth_enabled { return Ok(Principal::Anonymous); }
            if let Some(token_hash) = token_hash {
                let row: Option<(String, Vec<u8>, i64)> = self.connection.query_row(
                    "SELECT user, auth_revision, expires_at_ms FROM auth_tokens WHERE token_hash = ?1",
                    [token_hash], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                ).optional()?;
                let (name, revision, expiry) = row.ok_or_else(invalid_token)?;
                if expiry <= now_ms { return Err(invalid_token()); }
                return Ok(Principal::User { name, revision: super::decode_id(&revision)? });
            }
            if let Some(name) = certificate_name.filter(|name| !name.is_empty()) {
                return Ok(Principal::User { name: name.into(), revision: self.metadata.auth_revision });
            }
            Ok(Principal::Anonymous)
        })())
    }

    pub fn auth_query(&self, principal: &Principal, query: &AuthQuery) -> Result<AuthReply> {
        self.ready()?;
        self.guard(query_auth(
            &self.connection,
            &self.metadata,
            principal,
            query,
        ))
    }

    pub fn authorized_range(
        &self,
        principal: &Principal,
        request: &pb::RangeRequest,
    ) -> Result<pb::RangeResponse> {
        self.ready()?;
        let result = (|| {
            mvcc::validate_range(request)?;
            let transaction = self.connection.unchecked_transaction()?;
            let metadata = super::load_metadata(&transaction)?;
            authorize_range(
                &transaction,
                &metadata,
                principal,
                &request.key,
                &request.range_end,
                false,
            )?;
            let result = mvcc::range(&transaction, &metadata, request)?;
            transaction.commit()?;
            Ok(result)
        })();
        self.guard(result)
    }

    pub fn authorize_watch(&self, principal: &Principal, key: &[u8], end: &[u8]) -> Result<()> {
        self.ready()?;
        self.guard(authorize_range(
            &self.connection,
            &self.metadata,
            principal,
            key,
            end,
            false,
        ))
    }
}

pub(crate) fn require_user(
    connection: &Connection,
    metadata: &Metadata,
    principal: &Principal,
) -> Result<Option<authpb::User>> {
    if !metadata.auth_enabled || principal == &Principal::Internal {
        return Ok(None);
    }
    let Principal::User { name, revision } = principal else {
        return Err(RpcError::invalid("etcdserver: user name is empty"));
    };
    if name.is_empty() || *revision == 0 {
        return Err(RpcError::invalid("etcdserver: user name is empty"));
    }
    if *revision < metadata.auth_revision {
        return Err(RpcError::invalid(
            "etcdserver: revision of auth store is old",
        ));
    }
    Ok(Some(user(connection, name)?.ok_or_else(denied)?))
}

pub(crate) fn require_admin(
    connection: &Connection,
    metadata: &Metadata,
    principal: &Principal,
) -> Result<()> {
    if !metadata.auth_enabled || principal == &Principal::Internal {
        return Ok(());
    }
    let Principal::User { name, .. } = principal else {
        return Err(RpcError::invalid("etcdserver: user name is empty"));
    };
    let existing = user(connection, name)?.ok_or_else(user_not_found)?;
    if existing.roles.iter().any(|role| role == "root") {
        Ok(())
    } else {
        Err(denied())
    }
}

pub(crate) fn authorize_lease(
    connection: &Connection,
    metadata: &Metadata,
    principal: &Principal,
    id: i64,
    write: bool,
) -> Result<()> {
    require_user(connection, metadata, principal)?;
    for key in lease::keys(connection, id)? {
        authorize_range(connection, metadata, principal, &key, &[], write)?;
    }
    Ok(())
}

pub(crate) fn authorize_kv(
    connection: &Connection,
    metadata: &Metadata,
    principal: &Principal,
    request: &KvRequest,
) -> Result<()> {
    match request {
        KvRequest::Range(request) => authorize_range(
            connection,
            metadata,
            principal,
            &request.key,
            &request.range_end,
            false,
        ),
        KvRequest::Put(request) => {
            authorize_range(connection, metadata, principal, &request.key, &[], true)?;
            if request.prev_kv {
                authorize_range(connection, metadata, principal, &request.key, &[], false)?;
            }
            if request.lease != 0 {
                authorize_lease(connection, metadata, principal, request.lease, true)?;
            }
            Ok(())
        }
        KvRequest::Delete(request) => {
            authorize_range(
                connection,
                metadata,
                principal,
                &request.key,
                &request.range_end,
                true,
            )?;
            if request.prev_kv {
                authorize_range(
                    connection,
                    metadata,
                    principal,
                    &request.key,
                    &request.range_end,
                    false,
                )?;
            }
            Ok(())
        }
        KvRequest::Compact(_) => require_admin(connection, metadata, principal),
        KvRequest::Txn(request) => {
            for compare in &request.compare {
                authorize_range(
                    connection,
                    metadata,
                    principal,
                    &compare.key,
                    &compare.range_end,
                    false,
                )?;
            }
            for operation in request.success.iter().chain(&request.failure) {
                match operation
                    .request
                    .as_ref()
                    .ok_or_else(|| RpcError::invalid("etcdserver: key not found"))?
                {
                    pb::request_op::Request::RequestRange(request) => authorize_kv(
                        connection,
                        metadata,
                        principal,
                        &KvRequest::Range(request.clone()),
                    )?,
                    pb::request_op::Request::RequestPut(request) => authorize_kv(
                        connection,
                        metadata,
                        principal,
                        &KvRequest::Put(request.clone()),
                    )?,
                    pb::request_op::Request::RequestDeleteRange(request) => authorize_kv(
                        connection,
                        metadata,
                        principal,
                        &KvRequest::Delete(request.clone()),
                    )?,
                    pb::request_op::Request::RequestTxn(request) => authorize_kv(
                        connection,
                        metadata,
                        principal,
                        &KvRequest::Txn(request.clone()),
                    )?,
                }
            }
            Ok(())
        }
    }
}

fn query_auth(
    connection: &Connection,
    metadata: &Metadata,
    principal: &Principal,
    query: &AuthQuery,
) -> Result<AuthReply> {
    match query {
        AuthQuery::Status => Ok(AuthReply::Status(pb::AuthStatusResponse {
            header: header(metadata.revision),
            enabled: metadata.auth_enabled,
            auth_revision: metadata.auth_revision,
        })),
        AuthQuery::UserGet { name } => {
            if let Err(error) = require_admin(connection, metadata, principal) {
                if error.is_storage_fault() {
                    return Err(error);
                }
                if !matches!(principal, Principal::User { name: caller, .. } if caller == name) {
                    return Err(error);
                }
            }
            let existing = user(connection, name)?.ok_or_else(user_not_found)?;
            Ok(AuthReply::UserGet(pb::AuthUserGetResponse {
                header: header(metadata.revision),
                roles: existing.roles,
            }))
        }
        AuthQuery::UserList => {
            require_admin(connection, metadata, principal)?;
            Ok(AuthReply::UserList(pb::AuthUserListResponse {
                header: header(metadata.revision),
                users: names(connection, "auth_users")?,
            }))
        }
        AuthQuery::RoleGet { name } => {
            if let Err(error) = require_admin(connection, metadata, principal) {
                if error.is_storage_fault() {
                    return Err(error);
                }
                let current = require_user(connection, metadata, principal)?.ok_or_else(denied)?;
                if !current.roles.contains(name) {
                    return Err(denied());
                }
            }
            let existing = role(connection, name)?.ok_or_else(role_not_found)?;
            let permissions = if name == "root" {
                vec![authpb::Permission {
                    perm_type: authpb::permission::Type::Readwrite as i32,
                    key: vec![0],
                    range_end: vec![0],
                }]
            } else {
                existing.key_permission
            };
            Ok(AuthReply::RoleGet(pb::AuthRoleGetResponse {
                header: header(metadata.revision),
                perm: permissions,
            }))
        }
        AuthQuery::RoleList => {
            require_admin(connection, metadata, principal)?;
            Ok(AuthReply::RoleList(pb::AuthRoleListResponse {
                header: header(metadata.revision),
                roles: names(connection, "auth_roles")?,
            }))
        }
    }
}

fn user(connection: &Connection, name: &str) -> Result<Option<authpb::User>> {
    let row: Option<(Vec<u8>, Vec<u8>)> = connection
        .query_row(
            "SELECT user, checksum FROM auth_users WHERE name = ?1",
            [name],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    row.map(|(bytes, checksum)| {
        if digest(&[name.as_bytes(), &bytes]) != checksum {
            return Err(RpcError::corrupt("auth user checksum mismatch"));
        }
        let user = authpb::User::decode(bytes.as_slice())?;
        if user.name != name.as_bytes() {
            return Err(RpcError::corrupt("auth user identity mismatch"));
        }
        Ok(user)
    })
    .transpose()
}

fn role(connection: &Connection, name: &str) -> Result<Option<authpb::Role>> {
    let row: Option<(Vec<u8>, Vec<u8>)> = connection
        .query_row(
            "SELECT role, checksum FROM auth_roles WHERE name = ?1",
            [name],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    row.map(|(bytes, checksum)| {
        if digest(&[name.as_bytes(), &bytes]) != checksum {
            return Err(RpcError::corrupt("auth role checksum mismatch"));
        }
        let role = authpb::Role::decode(bytes.as_slice())?;
        if role.name != name.as_bytes() {
            return Err(RpcError::corrupt("auth role identity mismatch"));
        }
        Ok(role)
    })
    .transpose()
}

fn save_user(connection: &Connection, name: &str, user: &authpb::User) -> Result<()> {
    let bytes = user.encode_to_vec();
    connection.execute(
        "INSERT INTO auth_users(name, user, checksum) VALUES(?1, ?2, ?3)
        ON CONFLICT(name) DO UPDATE SET user = excluded.user, checksum = excluded.checksum",
        params![name, bytes, digest(&[name.as_bytes(), &bytes])],
    )?;
    Ok(())
}

fn save_role(connection: &Connection, name: &str, role: &authpb::Role) -> Result<()> {
    let bytes = role.encode_to_vec();
    connection.execute(
        "INSERT INTO auth_roles(name, role, checksum) VALUES(?1, ?2, ?3)
        ON CONFLICT(name) DO UPDATE SET role = excluded.role, checksum = excluded.checksum",
        params![name, bytes, digest(&[name.as_bytes(), &bytes])],
    )?;
    Ok(())
}

fn names(connection: &Connection, table: &str) -> Result<Vec<String>> {
    let mut statement = connection.prepare(&format!("SELECT name FROM {table} ORDER BY name"))?;
    Ok(statement
        .query_map([], |row| row.get(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?)
}

#[derive(Clone, Eq, PartialEq)]
enum Upper {
    End(Vec<u8>),
    Infinite,
}
impl Ord for Upper {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Infinite, Self::Infinite) => Ordering::Equal,
            (Self::Infinite, _) => Ordering::Greater,
            (_, Self::Infinite) => Ordering::Less,
            (Self::End(left), Self::End(right)) => left.cmp(right),
        }
    }
}
impl PartialOrd for Upper {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn upper(key: &[u8], end: &[u8]) -> Upper {
    if end == [0] {
        Upper::Infinite
    } else if end.is_empty() {
        let mut end = key.to_vec();
        end.push(0);
        Upper::End(end)
    } else {
        Upper::End(end.to_vec())
    }
}

pub(crate) fn authorize_range(
    connection: &Connection,
    metadata: &Metadata,
    principal: &Principal,
    key: &[u8],
    end: &[u8],
    write: bool,
) -> Result<()> {
    let Some(current) = require_user(connection, metadata, principal)? else {
        return Ok(());
    };
    if current.roles.iter().any(|role| role == "root") {
        return Ok(());
    }
    let target = upper(key, end);
    let mut permissions = Vec::new();
    for name in &current.roles {
        if let Some(role) = role(connection, name)? {
            for permission in role.key_permission {
                if permission.perm_type == authpb::permission::Type::Readwrite as i32
                    || permission.perm_type == if write { 1 } else { 0 }
                {
                    permissions.push((
                        permission.key.clone(),
                        upper(&permission.key, &permission.range_end),
                    ));
                }
            }
        }
    }
    permissions.sort_by(|left, right| left.0.cmp(&right.0));
    let mut frontier = Upper::End(key.to_vec());
    for (start, end) in permissions {
        let accessible = match &frontier {
            Upper::Infinite => true,
            Upper::End(frontier) => &start <= frontier,
        };
        if accessible {
            frontier = frontier.max(end);
        }
        if frontier >= target {
            return Ok(());
        }
    }
    Err(denied())
}

fn valid_permission(permission: &authpb::Permission) -> bool {
    authpb::permission::Type::try_from(permission.perm_type).is_ok()
        && (permission.range_end == [0]
            || permission.range_end.is_empty()
            || permission.key < permission.range_end)
}
fn protect(forbidden: bool) -> Result<()> {
    if forbidden {
        Err(RpcError::invalid("etcdserver: invalid auth management"))
    } else {
        Ok(())
    }
}
fn denied() -> RpcError {
    RpcError::new(Code::PermissionDenied, "etcdserver: permission denied")
}
fn user_not_found() -> RpcError {
    RpcError::new(Code::FailedPrecondition, "etcdserver: user name not found")
}
fn role_not_found() -> RpcError {
    RpcError::new(Code::FailedPrecondition, "etcdserver: role name not found")
}
fn auth_failed() -> RpcError {
    RpcError::invalid("etcdserver: authentication failed, invalid user ID or password")
}
fn invalid_token() -> RpcError {
    RpcError::new(Code::Unauthenticated, "etcdserver: invalid auth token")
}
