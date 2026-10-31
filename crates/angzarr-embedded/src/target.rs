//! Where a command for a domain is handled: the routing seam.
//!
//! Every command the kernel handles, the application's own and every
//! policy step's, names its domain in `cover.domain`, and the registry
//! resolves that name to a [`Target`]: a domain handled here (its handler
//! in this process, or in a child behind a local socket, the kernel's
//! pipeline over the kernel's store either way), or a domain whose streams
//! another coordinator owns, reached through a [`CoordinatorPort`] (a
//! distributed angzarr's `CommandHandlerCoordinatorService`, say). The
//! kernel knows only the port: what sits behind it is the composition
//! root's, so a transport is added without touching the kernel.
//!
//! A command for a remote domain is checked and identified here (its
//! command id fixed before it leaves, so a retry is deduplicated there; a
//! correlation its sender left unset is left unset, for the remote
//! coordinator's own default), then handed to the port ([`forward`]);
//! nothing of its stream is loaded, admitted, decided or written here, and
//! no policy or projector here handles its events
//! ([`Misconfigured::RemoteSource`](super::Misconfigured::RemoteSource)). Its answer
//! comes back as a local domain's would: its events, a duplicate, or a
//! refusal under the class its status gives. A policy's step into a remote
//! domain goes the same way, the policy's position written here.

use std::future::Future;
use std::pin::Pin;

use angzarr_proto::v1::angzarr_dead_letter::Payload;
use angzarr_proto::v1::{CommandBook, ComponentKind, DeliveryOutcome, EventBook};
use angzarr_router::error::CodedError;
use angzarr_router::pb::CommandResponse;
use angzarr_router::router::DynCommandHandler;

use super::check::Identified;
use super::coordinator::refused;
use super::dead_letter::{self, Cause, Component, DeadLetterLog};
use super::error::{Failure, KernelError};
use super::route::{self, Rejected};

/// What a [`CoordinatorPort`]'s call is: a `Send` future, so the kernel's
/// own future stays `Send` across it.
pub type Coordinated<'a> =
    Pin<Box<dyn Future<Output = Result<CommandResponse, CodedError>> + Send + 'a>>;

/// A coordinator elsewhere that owns a domain's streams, as the kernel
/// reaches it: angzarr's `CommandHandlerCoordinatorService.Handle`. It is
/// handed the book as the kernel identified it (its command id fixed
/// here, so a retry is deduplicated there; its correlation as its sender
/// set it, or unset) and answers as `Handle` does:
/// the persisted events, under `outcome.delivery` APPLIED or DUPLICATE; or
/// a refusal, as the router carries one ([`CodedError`]: a business
/// refusal's code and args, a bug, a retryable failure), which the kernel
/// treats as it treats a local handler's.
pub trait CoordinatorPort: Send + Sync {
    /// Handle `book`, which names one of this coordinator's domains.
    fn handle(&self, book: CommandBook) -> Coordinated<'_>;
}

/// What a domain name resolves to ([`Registry::target`](super::Registry::target)).
pub enum Target<'r> {
    /// Handled here: the kernel's pipeline over its own store, deciding
    /// through this handler (in this process, or a child's behind its
    /// socket).
    Local(&'r dyn DynCommandHandler),
    /// Handled by the coordinator that owns the domain's streams: nothing
    /// of it is loaded or written here.
    Remote(&'r dyn CoordinatorPort),
}

impl std::fmt::Debug for Target<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Target::Local(handler) => f.debug_tuple("Local").field(&handler.name()).finish(),
            Target::Remote(_) => f.write_str("Remote"),
        }
    }
}

/// What a remote coordinator's answer came to: the events it persisted for
/// the command's stream (perhaps none), or the ones it had already, a
/// duplicate.
pub(crate) enum Forwarded {
    Applied(EventBook),
    Duplicate(EventBook),
}

/// `command`, identified here, handed to the coordinator `port` that owns
/// its stream, and its answer in the kernel's terms: its events, under
/// their delivery; or its refusal as [`refused`] reads a local handler's,
/// a handler's bug dead-lettered by its code and args as a local one is.
/// An answer naming no stream is for the command's.
pub(crate) async fn forward(
    port: &dyn CoordinatorPort,
    command: &Identified,
    dead_letters: &dyn DeadLetterLog,
) -> Result<Forwarded, Failure<Rejected>> {
    match port.handle(command.to_remote_book()).await {
        Ok(response) => {
            let mut events = response.events.unwrap_or_default();
            if events.cover.is_none() {
                events.cover = Some(command.cover().clone());
            }
            let duplicate = response
                .outcome
                .is_some_and(|o| o.delivery == DeliveryOutcome::Duplicate as i32);
            Ok(match duplicate {
                true => Forwarded::Duplicate(events),
                false => Forwarded::Applied(events),
            })
        }
        Err(error) => {
            let failure = refused(command, error);
            if let Failure::Kernel(error @ KernelError::Router(coded)) = &failure {
                if route::is_handler_fault(coded) {
                    dead_letter::write(
                        dead_letters,
                        Payload::RejectedCommand(command.to_book()),
                        // Its kind is its coordinator's to say: unknown here.
                        Component {
                            name: command.domain(),
                            kind: ComponentKind::Unspecified,
                        },
                        Cause::Kernel(error),
                    );
                }
            }
            Err(failure)
        }
    }
}

#[cfg(test)]
#[path = "target.test.rs"]
mod tests;
