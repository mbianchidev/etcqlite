use super::Api;
use crate::{
    storage::{
        auth::{AuthCommand, AuthQuery, AuthReply, Principal},
        command::{Mutation, Reply},
    },
    wire::{authpb, etcdserverpb as pb},
};
use prost::Message;
use tonic::{Request, Response, Status};

impl Api {
    async fn auth_mutation(
        &self,
        principal: Principal,
        request: AuthCommand,
    ) -> std::result::Result<AuthReply, Status> {
        match self
            .runtime
            .mutate(principal, Mutation::Auth(request))
            .await
            .map_err(|error| error.status())?
        {
            Reply::Auth(reply) => Ok(reply),
            _ => Err(Status::internal(
                "auth command returned the wrong reply kind",
            )),
        }
    }

    async fn auth_read(
        &self,
        principal: Principal,
        query: AuthQuery,
    ) -> std::result::Result<AuthReply, Status> {
        self.runtime
            .barrier()
            .await
            .map_err(|error| error.status())?;
        self.runtime
            .read(move |store, state| {
                let mut reply = store.auth_query(&principal, &query)?;
                if let Some(header) = reply.header_mut() {
                    let physical = state.header(store);
                    header.cluster_id = physical.cluster_id;
                    header.member_id = physical.member_id;
                    header.raft_term = physical.raft_term;
                }
                Ok(reply)
            })
            .await
            .map_err(|error| error.status())
    }
}

macro_rules! implement_auth {
    (
        mutations { $(($method:ident, $request:ty, $response:ty, $variant:ident, $build:expr)),* $(,)? }
        queries { $(($query_method:ident, $query_request:ty, $query_response:ty, $query_variant:ident, $query_build:expr)),* $(,)? }
        custom { $($custom:item)* }
    ) => {
        #[tonic::async_trait]
        impl pb::auth_server::Auth for Api {
            $(async fn $method(&self, request: Request<$request>) -> std::result::Result<Response<$response>, Status> {
                let principal = self.checked(&request).await?;
                match self.auth_mutation(principal, ($build)(request.into_inner())).await? {
                    AuthReply::$variant(reply) => self.response(reply),
                    _ => Err(Status::internal("auth method returned an invalid response")),
                }
            })*
            $(async fn $query_method(&self, request: Request<$query_request>) -> std::result::Result<Response<$query_response>, Status> {
                let principal = self.checked(&request).await?;
                match self.auth_read(principal, ($query_build)(request.into_inner())).await? {
                    AuthReply::$query_variant(reply) => self.response(reply),
                    _ => Err(Status::internal("auth query returned an invalid response")),
                }
            })*
            $($custom)*
        }
    };
}

implement_auth! {
    mutations {
        (auth_enable, pb::AuthEnableRequest, pb::AuthEnableResponse, Enabled, |_: pb::AuthEnableRequest| AuthCommand::Enable),
        (auth_disable, pb::AuthDisableRequest, pb::AuthDisableResponse, Disabled, |_: pb::AuthDisableRequest| AuthCommand::Disable),
        (user_delete, pb::AuthUserDeleteRequest, pb::AuthUserDeleteResponse, UserDeleted, |request: pb::AuthUserDeleteRequest| AuthCommand::UserDelete { name: request.name }),
        (user_grant_role, pb::AuthUserGrantRoleRequest, pb::AuthUserGrantRoleResponse, UserRoleGranted, |request: pb::AuthUserGrantRoleRequest| AuthCommand::UserGrantRole { user: request.user, role: request.role }),
        (user_revoke_role, pb::AuthUserRevokeRoleRequest, pb::AuthUserRevokeRoleResponse, UserRoleRevoked, |request: pb::AuthUserRevokeRoleRequest| AuthCommand::UserRevokeRole { user: request.name, role: request.role }),
        (role_add, pb::AuthRoleAddRequest, pb::AuthRoleAddResponse, RoleAdded, |request: pb::AuthRoleAddRequest| AuthCommand::RoleAdd { name: request.name }),
        (role_delete, pb::AuthRoleDeleteRequest, pb::AuthRoleDeleteResponse, RoleDeleted, |request: pb::AuthRoleDeleteRequest| AuthCommand::RoleDelete { name: request.role }),
        (role_grant_permission, pb::AuthRoleGrantPermissionRequest, pb::AuthRoleGrantPermissionResponse, PermissionGranted, |request: pb::AuthRoleGrantPermissionRequest| AuthCommand::RoleGrantPermission { name: request.name, permission: request.perm }),
        (role_revoke_permission, pb::AuthRoleRevokePermissionRequest, pb::AuthRoleRevokePermissionResponse, PermissionRevoked, |request: pb::AuthRoleRevokePermissionRequest| AuthCommand::RoleRevokePermission { name: request.role, key: request.key, range_end: request.range_end }),
    }
    queries {
        (auth_status, pb::AuthStatusRequest, pb::AuthStatusResponse, Status, |_: pb::AuthStatusRequest| AuthQuery::Status),
        (user_get, pb::AuthUserGetRequest, pb::AuthUserGetResponse, UserGet, |request: pb::AuthUserGetRequest| AuthQuery::UserGet { name: request.name }),
        (user_list, pb::AuthUserListRequest, pb::AuthUserListResponse, UserList, |_: pb::AuthUserListRequest| AuthQuery::UserList),
        (role_get, pb::AuthRoleGetRequest, pb::AuthRoleGetResponse, RoleGet, |request: pb::AuthRoleGetRequest| AuthQuery::RoleGet { name: request.role }),
        (role_list, pb::AuthRoleListRequest, pb::AuthRoleListResponse, RoleList, |_: pb::AuthRoleListRequest| AuthQuery::RoleList),
    }
    custom {

    async fn user_add(&self, request: Request<pb::AuthUserAddRequest>) -> std::result::Result<Response<pb::AuthUserAddResponse>, Status> {
        let principal = self.checked(&request).await?;
        self.require_admin(principal.clone()).await?;
        let request = request.into_inner();
        let options = request.options.unwrap_or(authpb::UserAddOptions { no_password: false });
        let password_hash = self.password_hash(request.password, request.hashed_password, options.no_password).await?;
        match self.auth_mutation(principal, AuthCommand::UserAdd { name: request.name, password_hash, options }).await? {
            AuthReply::UserAdded(reply) => self.response(reply),
            _ => Err(Status::internal("invalid user-add response")),
        }
    }

    async fn user_change_password(&self, request: Request<pb::AuthUserChangePasswordRequest>) -> std::result::Result<Response<pb::AuthUserChangePasswordResponse>, Status> {
        let principal = self.checked(&request).await?;
        self.require_admin(principal.clone()).await?;
        let request = request.into_inner();
        let password_hash = self.password_hash(request.password, request.hashed_password, false).await?;
        match self.auth_mutation(principal, AuthCommand::UserChangePassword { name: request.name, password_hash }).await? {
            AuthReply::PasswordChanged(reply) => self.response(reply),
            _ => Err(Status::internal("invalid password-change response")),
        }
    }

    async fn authenticate(&self, request: Request<pb::AuthenticateRequest>) -> std::result::Result<Response<pb::AuthenticateResponse>, Status> {
        if request.get_ref().encoded_len() > crate::storage::MAX_REQUEST_BYTES { return Err(Status::invalid_argument("etcdserver: request is too large")); }
        let request = request.into_inner();
        self.response(Api::authenticate(self, request.name, request.password).await?)
    }
    }
}
