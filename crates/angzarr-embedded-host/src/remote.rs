//! A remote component as angzarr-router sees one: a domain (an aggregate
//! or a process manager) as a [`CommandHandler`] over
//! `CommandHandlerService`, a policy as a [`PolicyComponent`] over
//! `PolicyService`, a projector as a [`ProjectorHandler`] over
//! `ProjectorService`, each on its child's socket, its descriptor the one
//! its current child's `ComponentService.Describe` gave. A remote process
//! manager's start rule is the kernel's, from that descriptor's starting
//! types (decisions 14.41, 14.42): no call reaches the child.
//!
//! What comes back is in the router's terms:
//!
//! - every status keeps its gRPC code, so the status decides the error's
//!   class (decision 14.37), and its `google.rpc.ErrorInfo` detail's
//!   reason and metadata are the code and extras. The status's message is
//!   never forwarded: no text crosses;
//! - a business status (FAILED_PRECONDITION, INVALID_ARGUMENT, NOT_FOUND,
//!   ALREADY_EXISTS, OUT_OF_RANGE, PERMISSION_DENIED) is a refusal, and its
//!   other detail, an `Any`, the typed refusal ([`CodedError::refusal`]);
//!   any other status carries no typed refusal;
//! - a child that is not running, a connection that fails, and an
//!   UNAVAILABLE status with no details are
//!   [`codes::REMOTE_HANDLER_UNAVAILABLE`], which [`is_retryable`] says to
//!   retry: the child is being restarted. A transport's failure carries the
//!   transport's text verbatim as [`EXT_ERR_TXT`], a third party's;
//! - a status angzarr-router's `GrpcCode` has no variant for (CANCELLED,
//!   UNAUTHENTICATED) arrives UNAVAILABLE: a retryable bug, as the owner
//!   ruled.

use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use angzarr_router::context::CommandContext;
use angzarr_router::error::{CodedError, GrpcCode, RouterReason};
use angzarr_router::pb;
use angzarr_router::router::{CommandHandler, PolicyComponent, ProjectorHandler};
use angzarr_router::send::BoxFuture;
use prost::Message;
use tonic::transport::Channel;
use tonic::Code;

use crate::rpc::command_handler::command_handler_service_client::CommandHandlerServiceClient;
use crate::rpc::google_rpc::{ErrorInfo, Status as RpcStatus, ERROR_INFO_NAME};
use crate::rpc::policy::policy_service_client::PolicyServiceClient;
use crate::rpc::projector::projector_service_client::ProjectorServiceClient;
use crate::rpc::upcaster::upcaster_service_client::UpcasterServiceClient;
use angzarr_diagnostic::Report;
use angzarr_embedded::reasons::args;
use angzarr_embedded::{Description, KernelError, UpcastError, UpcastPort};

/// The router-style codes a remote handler's dispatch adds to the router's
/// own (`angzarr_router::error::codes`).
pub mod codes {
    /// The domain's child cannot be reached: not running (exited, being
    /// restarted), the connection failed, or it answered UNAVAILABLE.
    /// The spec's `COORDINATOR_REASON_HANDLER_UNAVAILABLE` (decision
    /// 14.49), UNAVAILABLE: retryable ([`super::is_retryable`]).
    pub const REMOTE_HANDLER_UNAVAILABLE: &str = "COORDINATOR_REASON_HANDLER_UNAVAILABLE";
}

/// The extras key naming why a remote handler was unreachable, one of the
/// [`causes`]. The domain is under the catalog's `domain`
/// (`angzarr_embedded::reasons::args::DOMAIN`).
pub const CAUSE: &str = "cause";

/// The extras key a transport's failure carries its text under, verbatim:
/// a third party's (tonic's, hyper's) words, never angzarr's.
pub const EXT_ERR_TXT: &str = "ext_err_txt";

/// The values of [`CAUSE`].
pub mod causes {
    /// No child is up: it exited and is being restarted, or, a sidecar,
    /// its health does not report SERVING.
    pub const NOT_RUNNING: &str = "not-running";
    /// The call did not reach the child, or its answer did not come back.
    pub const TRANSPORT: &str = "transport";
    /// The child did not answer within the call timeout
    /// (`Supervision::call_timeout_ms`).
    pub const TIMEOUT: &str = "timeout";
}

/// Whether a remote handler's error is worth retrying: the child was
/// unreachable, and will be back.
pub fn is_retryable(error: &CodedError) -> bool {
    error.code == codes::REMOTE_HANDLER_UNAVAILABLE
}

/// One remote component: its configured name, the descriptor `Describe`
/// gave when it first came up (what it is named, of which kind, and
/// routed by, for good), and the link to its current child.
struct Remote {
    name: String,
    descriptor: pb::ComponentDescriptor,
    link: Arc<Link>,
    call_timeout: Duration,
}

/// The connection to a component's current child, which its supervisor
/// sets when a child comes up and clears when it exits (a sidecar's, when
/// its health turns SERVING and when it turns otherwise).
pub(crate) struct Link {
    pub(crate) channel: RwLock<Option<Channel>>,
    /// What the component is registered by: its current child's
    /// descriptor, and once [`Host::register`](crate::Host::register) has
    /// registered a domain, its registration's [`Description`]. The
    /// supervisor replaces both, under this lock, before it publishes a
    /// child described again (C-0563), so registering never misses a
    /// child's descriptor and a child never gets work before it is
    /// registered.
    pub(crate) registered: Mutex<Registered>,
    /// How many times a child has come up: the supervisor counts each one
    /// after setting its channel (a sidecar's each time its health turns
    /// SERVING), so a watcher that sees `n` reaches the `n`th.
    pub(crate) generation: tokio::sync::watch::Sender<u64>,
}

impl Default for Link {
    fn default() -> Self {
        Link {
            channel: RwLock::new(None),
            registered: Mutex::new(Registered::default()),
            generation: tokio::sync::watch::channel(0).0,
        }
    }
}

/// What a component is registered by ([`Link::registered`]).
#[derive(Default)]
pub(crate) struct Registered {
    /// The descriptor its current child (or, while it is down, its last)
    /// answered `Describe` with.
    pub(crate) descriptor: pb::ComponentDescriptor,
    /// A registered domain's registration, which a child described again
    /// replaces its descriptor through.
    pub(crate) description: Option<Description>,
}

impl Link {
    /// [`Link::registered`], locked: never held across an await.
    pub(crate) fn registered(&self) -> std::sync::MutexGuard<'_, Registered> {
        self.registered
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl Remote {
    /// The descriptor its current child answered `Describe` with.
    fn current(&self) -> pb::ComponentDescriptor {
        self.link.registered().descriptor.clone()
    }

    /// `rpc` on the current child's channel, awaited on the caller's
    /// runtime until it answers or the call timeout passes (retryable). No
    /// thread is blocked: the caller's task yields while the call is out.
    /// The caller is on a tokio runtime with its timer enabled.
    async fn call<T, F, Fut>(&self, rpc: F) -> Result<T, CodedError>
    where
        F: FnOnce(Channel) -> Fut,
        Fut: std::future::Future<Output = Result<tonic::Response<T>, tonic::Status>>,
    {
        let name = &self.name;
        let channel = self
            .link
            .channel
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let Some(channel) = channel else {
            return Err(unavailable(name, causes::NOT_RUNNING));
        };
        match tokio::time::timeout(self.call_timeout, rpc(channel)).await {
            Ok(Ok(response)) => Ok(response.into_inner()),
            Ok(Err(status)) => Err(status_to_coded(name, &status)),
            Err(_elapsed) => Err(unavailable(name, causes::TIMEOUT)),
        }
    }

    /// The domain its descriptor names as its first input: a policy's
    /// source, its one. Empty for a domain.
    fn input(&self) -> &str {
        self.descriptor
            .inputs
            .first()
            .map_or("", |t| t.domain.as_str())
    }
}

/// A remote domain's handler (an aggregate or a process manager). Cloning
/// it is cheap; every clone reaches the same supervised child, whichever
/// process it currently is.
#[derive(Clone)]
pub struct RemoteCommandHandler {
    inner: Arc<Remote>,
}

impl RemoteCommandHandler {
    pub(crate) fn new(
        name: String,
        descriptor: pb::ComponentDescriptor,
        link: Arc<Link>,
        call_timeout: Duration,
    ) -> Self {
        RemoteCommandHandler {
            inner: Arc::new(Remote {
                name,
                descriptor,
                link,
                call_timeout,
            }),
        }
    }

    /// Watches how many children of this domain have come up.
    #[cfg(test)]
    pub(crate) fn generations(&self) -> tokio::sync::watch::Receiver<u64> {
        self.inner.link.generation.subscribe()
    }

    /// The domain this handler is configured for: its `[[component]]`
    /// name.
    pub fn domain(&self) -> &str {
        &self.inner.name
    }

    /// The descriptor its current child answered `Describe` with (its
    /// last, while it is down): what the kernel registers the domain by.
    pub fn descriptor(&self) -> pb::ComponentDescriptor {
        self.inner.current()
    }

    /// The link to its children, which [`Host::register`](crate::Host::register)
    /// registers the domain through.
    pub(crate) fn link(&self) -> &Link {
        &self.inner.link
    }
}

impl std::fmt::Debug for RemoteCommandHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteCommandHandler")
            .field("domain", &self.inner.name)
            .finish_non_exhaustive()
    }
}

impl CommandHandler for RemoteCommandHandler {
    /// The component's name, as its descriptor gives it (a domain's
    /// component is named for itself, not for the domain:
    /// `PlayerAggregate` for `player`).
    fn name(&self) -> &str {
        &self.inner.descriptor.name
    }

    /// The kind its descriptor gives.
    fn kind(&self) -> pb::ComponentKind {
        self.inner.descriptor.kind()
    }

    /// None: the wire descriptor carries no command types, and the router
    /// hosts one component, which every command reaches.
    fn command_types(&self) -> Vec<String> {
        Vec::new()
    }

    /// `CommandHandlerService.Handle` on the child, awaited until it
    /// answers or the call timeout passes (retryable).
    async fn dispatch(
        &self,
        req: &pb::ContextualCommand,
    ) -> Result<pb::CommandHandlerResponse, CodedError> {
        let req = req.clone();
        self.inner
            .call(move |channel| async move {
                CommandHandlerServiceClient::new(channel).handle(req).await
            })
            .await
    }
}

/// A remote policy's handler: its child serves `PolicyService`, and the
/// kernel runs it as any policy of its source
/// ([`PolicyComponent::input_domain`], its descriptor's one input).
/// Cloning it is cheap.
#[derive(Clone)]
pub struct RemotePolicyHandler {
    inner: Arc<Remote>,
}

impl RemotePolicyHandler {
    pub(crate) fn new(
        name: String,
        descriptor: pb::ComponentDescriptor,
        link: Arc<Link>,
        call_timeout: Duration,
    ) -> Self {
        RemotePolicyHandler {
            inner: Arc::new(Remote {
                name,
                descriptor,
                link,
                call_timeout,
            }),
        }
    }

    /// The `[[component]]` name it is configured as.
    pub fn configured(&self) -> &str {
        &self.inner.name
    }
}

impl std::fmt::Debug for RemotePolicyHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemotePolicyHandler")
            .field("component", &self.inner.name)
            .finish_non_exhaustive()
    }
}

impl PolicyComponent for RemotePolicyHandler {
    /// The component's name, as its descriptor gives it.
    fn name(&self) -> &str {
        &self.inner.descriptor.name
    }

    /// The domain whose events it handles: its descriptor's one input.
    fn input_domain(&self) -> &str {
        self.inner.input()
    }

    /// `PolicyService.Handle` on the child.
    async fn dispatch(&self, source: &pb::EventBook) -> Result<pb::PolicyResponse, CodedError> {
        let source = source.clone();
        self.inner
            .call(move |channel| async move {
                PolicyServiceClient::new(channel).handle(source).await
            })
            .await
    }

    /// `PolicyService.OnRefused` on the child, with the refusal (`ctx`
    /// stays here: the child's library rebuilds it from the refused
    /// command): its translated fact's payload, or `None` for an empty
    /// answer, no translation (decision 14.42). A translated fact that is
    /// more than a payload is the policy's bug, refused INTERNAL: one that
    /// sets its own cover or Handling (`COORDINATOR_REASON_REFUSAL_FACT_ADDRESSED`), has no
    /// page (`COORDINATOR_REASON_REFUSAL_FACT_NO_PAGE`) or a page that is not a fact
    /// (`COORDINATOR_REASON_REFUSAL_FACT_NOT_A_FACT`), or sets its own identity
    /// (`COORDINATOR_REASON_REFUSAL_FACT_IDENTIFIED`).
    async fn dispatch_refusal(
        &self,
        refusal: &pb::Refusal,
        _ctx: &CommandContext,
    ) -> Result<Option<prost_types::Any>, CodedError> {
        let refusal = refusal.clone();
        let answer = self
            .inner
            .call(move |channel| async move {
                PolicyServiceClient::new(channel).on_refused(refusal).await
            })
            .await?;
        let Some(fact) = answer.fact else {
            return Ok(None);
        };
        angzarr_embedded::translated(fact).map(Some)
    }
}

impl RemotePolicyHandler {
    /// The descriptor its current child answered `Describe` with (its
    /// last, while it is down).
    pub fn descriptor(&self) -> pb::ComponentDescriptor {
        self.inner.current()
    }
}

/// A remote upcaster sidecar (decision 14.39): its child serves
/// `UpcasterService` and gRPC health, for [`Self::upcasts`], and the kernel
/// upcasts that domain's inputs through it, as its `UpcastPort`, while its
/// health reports SERVING. Cloning it is cheap.
#[derive(Clone)]
pub struct RemoteUpcaster {
    inner: Arc<Remote>,
    upcasts: String,
}

impl RemoteUpcaster {
    pub(crate) fn new(
        name: String,
        upcasts: String,
        link: Arc<Link>,
        call_timeout: Duration,
    ) -> Self {
        RemoteUpcaster {
            inner: Arc::new(Remote {
                name,
                descriptor: pb::ComponentDescriptor::default(),
                link,
                call_timeout,
            }),
            upcasts,
        }
    }

    /// The `[[component]]` name it is configured as.
    pub fn configured(&self) -> &str {
        &self.inner.name
    }

    /// The domain whose payloads it upcasts.
    pub fn upcasts(&self) -> &str {
        &self.upcasts
    }
}

impl std::fmt::Debug for RemoteUpcaster {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteUpcaster")
            .field("component", &self.inner.name)
            .field("upcasts", &self.upcasts)
            .finish_non_exhaustive()
    }
}

impl UpcastPort for RemoteUpcaster {
    /// `UpcasterService.Upcast` on the child. A child that cannot be
    /// reached ([`is_retryable`]) is not serving: [`UpcastError::Unavailable`],
    /// retried. Any other status is the failure of the rule it ran on the
    /// request's first payload, a bug ([`UpcastError::Rule`]), carrying the
    /// child's reason.
    fn upcast<'a>(
        &'a self,
        request: &'a pb::UpcastRequest,
    ) -> BoxFuture<'a, Result<pb::UpcastResponse, UpcastError>> {
        Box::pin(async move {
            let sent = request.clone();
            self.inner
                .call(move |channel| async move {
                    UpcasterServiceClient::new(channel).upcast(sent).await
                })
                .await
                .map_err(|error| self.refused(request, error))
        })
    }
}

impl RemoteUpcaster {
    /// The sidecar's failed `Upcast` of `request`, in the kernel's terms
    /// (see [`UpcastPort::upcast`]).
    fn refused(&self, request: &pb::UpcastRequest, error: CodedError) -> UpcastError {
        if is_retryable(&error) {
            let cause = error.extras.get("cause").cloned().unwrap_or_default();
            return UpcastError::Unavailable {
                cause: Report::of(&KernelError::HandlerUnavailable {
                    stream: pb::Cover {
                        domain: self.upcasts.clone(),
                        ..Default::default()
                    },
                    cause,
                }),
            };
        }
        let source = request
            .events
            .first()
            .and_then(|page| page.payload.as_ref())
            .map(|payload| angzarr_embedded::type_name(&payload.type_url).to_string())
            .unwrap_or_default();
        UpcastError::Rule {
            source,
            error: Report::of(&KernelError::Router(error)),
        }
    }
}

/// A remote projector's handler: its child serves `ProjectorService`, and
/// the kernel runs it after each commit that records events of any of its
/// [`Self::sources`]. Cloning it is cheap.
#[derive(Clone)]
pub struct RemoteProjectorHandler {
    inner: Arc<Remote>,
}

impl RemoteProjectorHandler {
    pub(crate) fn new(
        name: String,
        descriptor: pb::ComponentDescriptor,
        link: Arc<Link>,
        call_timeout: Duration,
    ) -> Self {
        RemoteProjectorHandler {
            inner: Arc::new(Remote {
                name,
                descriptor,
                link,
                call_timeout,
            }),
        }
    }

    /// The `[[component]]` name it is configured as.
    pub fn configured(&self) -> &str {
        &self.inner.name
    }

    /// The domains whose events it projects: its descriptor's inputs, in
    /// order.
    pub fn sources(&self) -> Vec<&str> {
        self.inner
            .descriptor
            .inputs
            .iter()
            .map(|input| input.domain.as_str())
            .collect()
    }
}

impl std::fmt::Debug for RemoteProjectorHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteProjectorHandler")
            .field("component", &self.inner.name)
            .finish_non_exhaustive()
    }
}

impl ProjectorHandler for RemoteProjectorHandler {
    /// The component's name, as its descriptor gives it.
    fn name(&self) -> &str {
        &self.inner.descriptor.name
    }

    /// `ProjectorService.Handle` on the child.
    async fn dispatch(&self, events: &pb::EventBook) -> Result<pb::Projection, CodedError> {
        let events = events.clone();
        self.inner
            .call(move |channel| async move {
                ProjectorServiceClient::new(channel).handle(events).await
            })
            .await
    }
}

/// A non-OK status from the child's `Handle`, in the router's terms (see
/// the module's docs).
pub(crate) fn status_to_coded(domain: &str, status: &tonic::Status) -> CodedError {
    let details = RpcStatus::decode(status.details())
        .map(|rpc| rpc.details)
        .unwrap_or_default();
    // A status the child never sent: the call failed on the way (the
    // connection refused or reset, the child gone mid-call), which tonic
    // reports with the transport's error as its source.
    let transport = details.is_empty()
        && (status.code() == Code::Unavailable
            || (status.code() == Code::Unknown && std::error::Error::source(status).is_some()));
    if transport {
        let mut error = unavailable(domain, causes::TRANSPORT);
        error
            .extras
            .insert(EXT_ERR_TXT.to_string(), transport_text(status));
        return error;
    }
    let is_info = |detail: &&prost_types::Any| {
        angzarr_router::type_name_from_url(&detail.type_url) == ERROR_INFO_NAME
    };
    let info = details
        .iter()
        .find(is_info)
        .and_then(|detail| ErrorInfo::decode(detail.value.as_slice()).ok());
    let refusal = details
        .iter()
        .find(|detail| !is_info(detail))
        .cloned()
        .map(Box::new);
    let grpc = match status.code() {
        Code::InvalidArgument => GrpcCode::InvalidArgument,
        Code::NotFound => GrpcCode::NotFound,
        Code::FailedPrecondition => GrpcCode::FailedPrecondition,
        Code::AlreadyExists => GrpcCode::AlreadyExists,
        Code::OutOfRange => GrpcCode::OutOfRange,
        Code::PermissionDenied => GrpcCode::PermissionDenied,
        Code::Aborted => GrpcCode::Aborted,
        Code::Unimplemented => GrpcCode::Unimplemented,
        Code::DataLoss => GrpcCode::DataLoss,
        Code::Internal => GrpcCode::Internal,
        Code::Unknown => GrpcCode::Unknown,
        Code::DeadlineExceeded => GrpcCode::DeadlineExceeded,
        Code::ResourceExhausted => GrpcCode::ResourceExhausted,
        // The statuses angzarr-router's GrpcCode has no variant for
        // (CANCELLED, UNAUTHENTICATED): retryable bugs, as UNAVAILABLE is.
        _ => GrpcCode::Unavailable,
    };
    let (code, extras) = match info {
        Some(info) => (info.reason, info.metadata.into_iter().collect()),
        None => (
            RouterReason::UnhandledHandlerError
                .as_str_name()
                .to_string(),
            Default::default(),
        ),
    };
    CodedError {
        code,
        extras,
        grpc,
        // Only a business status carries its typed object to a policy.
        refusal: refusal.filter(|_| {
            angzarr_embedded::class(grpc as i32) == angzarr_embedded::Class::BusinessFatal
        }),
    }
}

/// The transport's own text for a status the call failed with on the
/// way: its source's, else the status's message (tonic's).
fn transport_text(status: &tonic::Status) -> String {
    match std::error::Error::source(status) {
        Some(source) => source.to_string(),
        None => status.message().to_string(),
    }
}

/// [`codes::REMOTE_HANDLER_UNAVAILABLE`] for `domain`, for `cause`.
pub(crate) fn unavailable(domain: &str, cause: &str) -> CodedError {
    CodedError {
        code: codes::REMOTE_HANDLER_UNAVAILABLE.to_string(),
        extras: [
            (args::DOMAIN.to_string(), domain.to_string()),
            (CAUSE.to_string(), cause.to_string()),
        ]
        .into(),
        grpc: GrpcCode::Unavailable,
        refusal: None,
    }
}

#[cfg(test)]
#[path = "remote.test.rs"]
mod tests;
