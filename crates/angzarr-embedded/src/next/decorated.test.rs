//! A **wallet** and a **lamp** written entirely in angzarr-embedded-macros' markup,
//! beside a hand-written **tally**, in one registry; a **buy-in**, a
//! process manager, and the **policies** between it and the wallet. All are
//! on angzarr-embedded as a dependent sees it, `::angzarr-embedded`, the path the expansion
//! names: not `crate::`, whose types are another instance's in this test
//! build.
//!
//! The wallet is the markup's whole shape: the instance holds the state
//! (backed by a [`Ledger`], which knows nothing of the markup, and an
//! immutable [`WalletConfig`]); pure `&self` handlers that take a
//! [`Context`] and return their events as a single event, a tuple, an
//! `Option`, a `Vec`, or an [`Emit`] with a continuation step; a command
//! handler declaring its typed refusal objects (`refuses`); a fact
//! handler, with no `Result`, that records a refusal as an event; `&mut
//! self` appliers; a rejection inferred from the command handlers; a
//! snapshot form restored onto the factory's instance; domain types, each
//! with the wire message it travels as. Both marked-up aggregates are
//! registered by `Registry::aggregate`, with their factories. The lamp is the
//! smallest aggregate: no snapshot form, and no rejection it can return.
//!
//! What is pinned: routing by type URL; the router applying what a handler
//! returns, in order; continuations (a step sees the state after the
//! events before it, a step's rejection refuses the whole decision, the
//! step limit, the idempotence helper); every field of the context; replay
//! building a fresh instance from the registry's factory before the handler
//! runs, and a resume keeping the factory's configuration; a rejection
//! committing nothing, a message only or a typed refusal object; a fact
//! recording its outcome where a command would be refused; snapshots with
//! and without a snapshot form; the generated `WalletHandler` trait and
//! constants; the hand-written domain working beside the generated ones;
//! the process manager as a domain (its root the correlation, its start
//! rule, its one context); what the policies answer (their commands, the
//! refusal pass-through and translate) and the source's `#[on_refused]`;
//! and the projectors. What does not
//! compile is pinned by angzarr-embedded-macros' `compile_fail` examples.

use std::convert::Infallible;
use std::fmt;

use ::angzarr_embedded::next::markup::v1::{refusal_response, RefusalResponse};
use ::angzarr_embedded::next::markup::{
    self, assert_step_idempotent, BusinessStatus, CommandContext, CommandRefused, Commands,
    Context, DomainMessage, Emit, Failed, Policy, PolicyContext, ProcessManager, ProjectionContext,
    Record, Refusal, Refuses, Refusing, Rejection, RetryableStatus, TypeName, STEP_LIMIT,
};
use ::angzarr_embedded::next::v1::event_meta::Causation;
use ::angzarr_embedded::next::v1::{
    command_page, Command, CommandBook, CommandPage, ConflictResolution, Cover, EventBook,
    EventPage, EventRef, Fact, FactOrigin, StateDescriptor, StateOptions, Uuid,
};
use ::angzarr_embedded::next::{
    pack, type_name, Any, Decoder, Domain, Envelope, Executed, Failure, Handled, KernelError,
    MemoryDeadLetters, Payload, PayloadError, Recovery, Registry, Rejected, Sampled, State,
    StepError, Stream, Timestamp, Upcaster, WireError,
};
use angzarr_diagnostic::{Arg, Diagnostic, Report};
use angzarr_embedded_macros::{
    aggregate, applies, handles_cmd, handles_fact, on_event, on_refused, policy, process_manager,
    projector, projects,
};

// ---------------------------------------------------------------- the wire

/// The prost messages the toys' types travel as. (A fixture standing in
/// for angzarr's generated wire types.)
mod wire {
    use ::angzarr_embedded::next::Timestamp;

    macro_rules! message {
        ($package:literal, $name:ident { $($(#[$attr:meta])* $field:ident : $ty:ty),* $(,)? }) => {
            #[derive(Clone, PartialEq, ::prost::Message)]
            pub struct $name {
                $($(#[$attr])* pub $field: $ty,)*
            }

            impl ::prost::Name for $name {
                const NAME: &'static str = stringify!($name);
                const PACKAGE: &'static str = $package;
            }
        };
    }
    pub(super) use message;

    message!(
        "toy.wallet.v1",
        WalletSnapshot {
            #[prost(bool, tag = "1")]
            open: bool,
            #[prost(bool, tag = "2")]
            closed: bool,
            #[prost(int64, tag = "3")]
            cents: i64,
            #[prost(message, optional, tag = "4")]
            last_moved_at: Option<Timestamp>,
            #[prost(bool, tag = "5")]
            rewarded: bool,
        }
    );

    message!(
        "toy.wallet.cmd.v1",
        Open {
            #[prost(int64, tag = "1")]
            bonus: i64,
        }
    );
    message!(
        "toy.wallet.cmd.v1",
        Deposit {
            #[prost(int64, tag = "1")]
            cents: i64,
        }
    );
    message!(
        "toy.wallet.cmd.v1",
        Withdraw {
            #[prost(int64, tag = "1")]
            cents: i64,
        }
    );
    message!(
        "toy.wallet.cmd.v1",
        Sweep {
            #[prost(int64, tag = "1")]
            chunk: i64,
        }
    );
    message!("toy.wallet.cmd.v1", Close {});
    message!("toy.wallet.cmd.v1", Note {});
    message!("toy.wallet.cmd.v1", Spin {});

    message!("toy.wallet.v1", Opened {});
    message!(
        "toy.wallet.v1",
        Deposited {
            #[prost(int64, tag = "1")]
            cents: i64,
            #[prost(message, optional, tag = "2")]
            at: Option<Timestamp>,
        }
    );
    message!(
        "toy.wallet.v1",
        Rewarded {
            #[prost(int64, tag = "1")]
            cents: i64,
        }
    );
    message!(
        "toy.wallet.v1",
        Withdrew {
            #[prost(int64, tag = "1")]
            cents: i64,
        }
    );
    message!("toy.wallet.v1", Closed {});
    message!(
        "toy.wallet.v1",
        Noted {
            #[prost(bytes = "vec", tag = "1")]
            root: Vec<u8>,
            #[prost(bytes = "vec", tag = "2")]
            command_id: Vec<u8>,
            #[prost(bytes = "vec", tag = "3")]
            correlation_id: Vec<u8>,
        }
    );
    message!("toy.wallet.v1", Spun {});

    message!(
        "toy.wallet.fact.v1",
        Payout {
            #[prost(int64, tag = "1")]
            cents: i64,
        }
    );
    message!(
        "toy.wallet.fact.v1",
        Gift {
            #[prost(int64, tag = "1")]
            cents: i64,
        }
    );
    // An old spelling of `Deposited`, before `at` was recorded: what an
    // upcaster reads.
    message!(
        "toy.wallet.v0",
        OldDeposited {
            #[prost(int64, tag = "1")]
            cents: i64,
        }
    );
    message!(
        "toy.wallet.v1",
        PaidOut {
            #[prost(int64, tag = "1")]
            cents: i64,
        }
    );
    message!(
        "toy.wallet.v1",
        PayoutRefused {
            #[prost(int64, tag = "1")]
            cents: i64,
            #[prost(int64, tag = "2")]
            short: i64,
        }
    );

    message!(
        "toy.meter.cmd.v1",
        Gauge {
            #[prost(int64, tag = "1")]
            reading: i64,
        }
    );
    message!(
        "toy.meter.v1",
        Gauged {
            #[prost(int64, tag = "1")]
            reading: i64,
        }
    );
    message!(
        "toy.meter.cmd.v1",
        Tune {
            #[prost(int64, tag = "1")]
            reading: i64,
        }
    );
    message!(
        "toy.meter.cmd.v1",
        Seal {
            #[prost(int64, tag = "1")]
            code: i64,
        }
    );
    message!("toy.meter.v1", Sealed {});
    message!("toy.meter.v1", MeterSealed {});
    message!("toy.meter.cmd.v1", Calibrate {});
    message!("toy.meter.v1", Calibrated {});
    message!(
        "toy.meter.cmd.v1",
        Probe {
            #[prost(int64, tag = "1")]
            status: i64,
            #[prost(bool, tag = "2")]
            step: bool,
        }
    );
    message!("toy.meter.v1", Probed {});

    message!("toy.lamp.cmd.v1", SwitchOn {});
    message!("toy.lamp.v1", SwitchedOn {});

    message!(
        "toy.ladder.cmd.v1",
        Climb {
            #[prost(uint32, tag = "1")]
            to: u32,
        }
    );
    message!("toy.ladder.fact.v1", Slip {});
    message!("toy.ladder.v1", Rung {});

    message!(
        "toy.wallet.cmd.v1",
        Hold {
            #[prost(int64, tag = "1")]
            cents: i64,
        }
    );
    message!(
        "toy.wallet.v1",
        Held {
            #[prost(int64, tag = "1")]
            cents: i64,
        }
    );
    // The wallet's typed refusal objects for `Hold`.
    message!(
        "toy.wallet.v1",
        InsufficientFunds {
            #[prost(int64, tag = "1")]
            short: i64,
        }
    );
    message!("toy.wallet.v1", WalletClosed {});

    message!(
        "toy.buyin.cmd.v1",
        StartBuyIn {
            #[prost(bytes = "vec", tag = "1")]
            wallet: Vec<u8>,
            #[prost(int64, tag = "2")]
            cents: i64,
        }
    );
    message!(
        "toy.buyin.cmd.v1",
        RecordHeld {
            #[prost(int64, tag = "1")]
            cents: i64,
        }
    );
    message!("toy.buyin.cmd.v1", BuyInExpired {});
    message!(
        "toy.buyin.cmd.v1",
        HoldLost {
            #[prost(int64, tag = "1")]
            short: i64,
        }
    );
    message!(
        "toy.buyin.v1",
        BuyInStarted {
            #[prost(bytes = "vec", tag = "1")]
            wallet: Vec<u8>,
            #[prost(int64, tag = "2")]
            cents: i64,
        }
    );
    message!(
        "toy.buyin.v1",
        BuyInHeld {
            #[prost(int64, tag = "1")]
            cents: i64,
        }
    );
    message!(
        "toy.buyin.v1",
        Expired {
            #[prost(string, tag = "1")]
            external_id: String,
        }
    );
    message!(
        "toy.buyin.v1",
        BuyInFailed {
            #[prost(int64, tag = "1")]
            short: i64,
            #[prost(bool, tag = "2")]
            had_history: bool,
        }
    );
    message!(
        "toy.buyin.v1",
        HoldWasLost {
            #[prost(int64, tag = "1")]
            short: i64,
        }
    );

    message!(
        "toy.pot.cmd.v1",
        Stake {
            #[prost(int64, tag = "1")]
            cents: i64,
            #[prost(bool, tag = "2")]
            acknowledge: bool,
        }
    );
    message!("toy.pot.cmd.v1", Mark {});
    message!("toy.pot.cmd.v1", Peek {});
    message!(
        "toy.pot.v1",
        Staked {
            #[prost(int64, tag = "1")]
            cents: i64,
            #[prost(uint32, tag = "2")]
            window: u32,
        }
    );
    message!(
        "toy.pot.v1",
        Marked {
            #[prost(bytes = "vec", tag = "1")]
            root: Vec<u8>,
            #[prost(string, tag = "2")]
            domain: String,
        }
    );
    message!(
        "toy.pot.v1",
        Peeked {
            #[prost(uint64, tag = "1")]
            next_sequence: u64,
            #[prost(bool, tag = "2")]
            had_prior_events: bool,
            #[prost(uint64, tag = "3")]
            expected_sequence: u64,
            #[prost(int32, tag = "4")]
            resolution: i32,
            #[prost(bool, tag = "5")]
            conflict: bool,
        }
    );
}

// ---------------------------------------------------------------- the domain types

/// A domain type whose fields are its wire message's, one for one. (A
/// fixture standing in for angzarr-codegen's domain layer.)
macro_rules! domain_type {
    ($name:ident { $($field:ident : $ty:ty),* $(,)? }) => {
        #[derive(Clone, PartialEq, Debug)]
        pub struct $name {
            $(pub $field: $ty,)*
        }

        impl DomainMessage for $name {
            type Wire = wire::$name;

            fn to_wire(&self) -> wire::$name {
                wire::$name { $($field: self.$field.clone(),)* }
            }

            #[allow(unused_variables)]
            fn from_wire(wire: wire::$name) -> Result<Self, WireError> {
                Ok($name { $($field: wire.$field,)* })
            }
        }
    };
}

domain_type!(Open { bonus: i64 });
domain_type!(Deposit { cents: i64 });
domain_type!(Withdraw { cents: i64 });
domain_type!(Sweep { chunk: i64 });
domain_type!(Close {});
domain_type!(Note {});
domain_type!(Spin {});
domain_type!(Opened {});
domain_type!(Rewarded { cents: i64 });
domain_type!(Withdrew { cents: i64 });
domain_type!(Closed {});
domain_type!(Noted {
    root: Vec<u8>,
    command_id: Vec<u8>,
    correlation_id: Vec<u8>,
});
domain_type!(Spun {});
domain_type!(Payout { cents: i64 });
domain_type!(Gift { cents: i64 });
domain_type!(PaidOut { cents: i64 });
domain_type!(PayoutRefused {
    cents: i64,
    short: i64
});
domain_type!(SwitchOn {});
domain_type!(SwitchedOn {});
domain_type!(Hold { cents: i64 });
domain_type!(Held { cents: i64 });
domain_type!(InsufficientFunds { short: i64 });
domain_type!(WalletClosed {});
domain_type!(StartBuyIn {
    wallet: Vec<u8>,
    cents: i64
});
domain_type!(RecordHeld { cents: i64 });
domain_type!(BuyInExpired {});
domain_type!(HoldLost { short: i64 });
domain_type!(BuyInStarted {
    wallet: Vec<u8>,
    cents: i64
});
domain_type!(BuyInHeld { cents: i64 });
domain_type!(Expired {
    external_id: String
});
domain_type!(BuyInFailed {
    short: i64,
    had_history: bool
});
domain_type!(HoldWasLost { short: i64 });
domain_type!(Stake {
    cents: i64,
    acknowledge: bool
});
domain_type!(Mark {});
domain_type!(Peek {});
domain_type!(Staked {
    cents: i64,
    window: u32
});
domain_type!(Marked {
    root: Vec<u8>,
    domain: String
});
domain_type!(Peeked {
    next_sequence: u64,
    had_prior_events: bool,
    expected_sequence: u64,
    resolution: i32,
    conflict: bool,
});

/// A typed refusal object is a domain type and a diagnostic: its message
/// is the refusal's.
impl fmt::Display for InsufficientFunds {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} cents short of the hold", self.short)
    }
}

impl Diagnostic for InsufficientFunds {
    fn code(&self) -> &str {
        "toy-wallet-insufficient-funds"
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        vec![("short", self.short.into())]
    }
}

impl fmt::Display for WalletClosed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the wallet is closed")
    }
}

impl Diagnostic for WalletClosed {
    fn code(&self) -> &str {
        "toy-wallet-closed"
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        vec![]
    }
}

/// A domain type that is not its wire message: `at` is required, where the
/// wire's is optional.
#[derive(Clone, PartialEq, Debug)]
pub struct Deposited {
    pub cents: i64,
    pub at: Timestamp,
}

impl DomainMessage for Deposited {
    type Wire = wire::Deposited;

    fn to_wire(&self) -> wire::Deposited {
        wire::Deposited {
            cents: self.cents,
            at: Some(self.at),
        }
    }

    fn from_wire(wire: wire::Deposited) -> Result<Self, WireError> {
        Ok(Deposited {
            cents: wire.cents,
            at: wire.at.ok_or(WireError::Missing {
                message: "toy.wallet.v1.Deposited",
                field: "at",
            })?,
        })
    }
}

#[derive(Clone, PartialEq, Debug)]
pub enum Refused {
    NotPositive { cents: i64 },
    AlreadyOpen,
    NotOpen,
    Overdrawn { short: i64 },
    NotEmpty { cents: i64 },
    OverLimit { cents: i64, limit: i64 },
}

impl fmt::Display for Refused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Refused::NotPositive { cents } => write!(f, "{cents} cents is not an amount"),
            Refused::AlreadyOpen => f.write_str("the wallet is already open"),
            Refused::NotOpen => f.write_str("the wallet is not open"),
            Refused::Overdrawn { short } => write!(f, "{short} cents short"),
            Refused::NotEmpty { cents } => write!(f, "{cents} cents are still in the wallet"),
            Refused::OverLimit { cents, limit } => {
                write!(f, "{cents} cents is over the limit of {limit}")
            }
        }
    }
}

impl Diagnostic for Refused {
    fn code(&self) -> &str {
        match self {
            Refused::NotPositive { .. } => "toy-wallet-not-positive",
            Refused::AlreadyOpen => "toy-wallet-already-open",
            Refused::NotOpen => "toy-wallet-not-open",
            Refused::Overdrawn { .. } => "toy-wallet-overdrawn",
            Refused::NotEmpty { .. } => "toy-wallet-not-empty",
            Refused::OverLimit { .. } => "toy-wallet-over-limit",
        }
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        match *self {
            Refused::NotPositive { cents } | Refused::NotEmpty { cents } => {
                vec![("cents", cents.into())]
            }
            Refused::Overdrawn { short } => vec![("short", short.into())],
            Refused::OverLimit { cents, limit } => {
                vec![("cents", cents.into()), ("limit", limit.into())]
            }
            Refused::AlreadyOpen | Refused::NotOpen => vec![],
        }
    }
}

// ---------------------------------------------------------------- the wallet

/// What the wallet holds, and the rules of moving it: one of the objects
/// behind the aggregate. It knows nothing of commands, events or the
/// markup.
#[derive(Clone, Default, PartialEq, Debug)]
pub struct Ledger {
    cents: i64,
    last_moved_at: Option<Timestamp>,
}

impl Ledger {
    fn balance(&self) -> i64 {
        self.cents
    }

    fn covers(&self, cents: i64) -> Result<(), Refused> {
        match cents > self.cents {
            true => Err(Refused::Overdrawn {
                short: cents - self.cents,
            }),
            false => Ok(()),
        }
    }

    /// `balance` in pieces of at most `chunk`, largest first.
    fn pieces(&self, chunk: i64) -> Vec<i64> {
        let mut left = self.cents;
        let mut pieces = Vec::new();
        while left > 0 {
            pieces.push(left.min(chunk));
            left -= chunk;
        }
        pieces
    }

    fn credit(&mut self, cents: i64, at: Timestamp) {
        self.cents += cents;
        self.last_moved_at = Some(at);
    }

    fn bonus(&mut self, cents: i64) {
        self.cents += cents;
    }

    fn debit(&mut self, cents: i64) {
        self.cents -= cents;
    }
}

/// Immutable reference configuration a wallet is built with: the most it
/// may hold. Not state: no event changes it, and no snapshot holds it.
#[derive(Clone, PartialEq, Debug)]
pub struct WalletConfig {
    limit: i64,
}

impl Default for WalletConfig {
    fn default() -> Self {
        WalletConfig { limit: 1000 }
    }
}

/// The balance at which a wallet is rewarded, once.
const REWARD_AT: i64 = 100;

/// The aggregate: its state is its fields, and the [`Ledger`] behind it
/// does the arithmetic. The aggregate orchestrates.
#[derive(Clone, Default, PartialEq, Debug)]
pub struct Wallet {
    open: bool,
    closed: bool,
    rewarded: bool,
    ledger: Ledger,
    config: WalletConfig,
}

impl Wallet {
    /// A fresh wallet with `config`: what the registry's factory builds.
    pub fn with(config: WalletConfig) -> Wallet {
        Wallet {
            config,
            ..Wallet::default()
        }
    }
}

/// The snapshot form, both ways (this and [`Wallet::restore`]): the macro
/// builds the `State` impl from them.
impl From<&Wallet> for wire::WalletSnapshot {
    fn from(wallet: &Wallet) -> Self {
        wire::WalletSnapshot {
            open: wallet.open,
            closed: wallet.closed,
            cents: wallet.ledger.cents,
            last_moved_at: wallet.ledger.last_moved_at,
            rewarded: wallet.rewarded,
        }
    }
}

impl Wallet {
    /// Take the state a snapshot holds, keeping this instance's
    /// configuration (the factory's), which no snapshot holds.
    fn restore(&mut self, snapshot: wire::WalletSnapshot) -> Result<(), WireError> {
        self.open = snapshot.open;
        self.closed = snapshot.closed;
        self.rewarded = snapshot.rewarded;
        self.ledger = Ledger {
            cents: snapshot.cents,
            last_moved_at: snapshot.last_moved_at,
        };
        Ok(())
    }
}

#[aggregate(domain = "wallet", snapshot = wire::WalletSnapshot)]
impl Wallet {
    /// Two events, a tuple: the router applies them in order.
    #[handles_cmd(Open)]
    fn open(&self, cmd: Open, ctx: &Context) -> Result<(Opened, Deposited), Refused> {
        if self.open {
            return Err(Refused::AlreadyOpen);
        }
        Ok((
            Opened {},
            Deposited {
                cents: cmd.bonus,
                at: ctx.recorded_at(),
            },
        ))
    }

    /// One event, stamped with the context's instant (the handler has no
    /// clock), then a continuation: whether the deposit is allowed, or
    /// earns the reward, depends on the balance after it.
    #[handles_cmd(Deposit)]
    fn deposit(&self, cmd: Deposit, ctx: &Context) -> Result<Emit<Self>, Refused> {
        if !self.open {
            return Err(Refused::NotOpen);
        }
        positive(cmd.cents)?;
        Ok(Emit::of(Deposited {
            cents: cmd.cents,
            at: ctx.recorded_at(),
        })
        .then(Self::settle))
    }

    /// A continuation step: run on the wallet with the deposit applied.
    /// Over the configured limit refuses the whole decision; at
    /// [`REWARD_AT`] or more, a wallet not yet rewarded earns a tenth.
    /// Idempotent: once rewarded, it emits nothing.
    fn settle(&self, _ctx: &Context) -> Result<Option<Rewarded>, Refused> {
        let cents = self.ledger.balance();
        if cents > self.config.limit {
            return Err(Refused::OverLimit {
                cents,
                limit: self.config.limit,
            });
        }
        Ok((cents >= REWARD_AT && !self.rewarded).then_some(Rewarded { cents: cents / 10 }))
    }

    #[handles_cmd(Withdraw)]
    fn withdraw(&self, cmd: Withdraw, _ctx: &Context) -> Result<Withdrew, Refused> {
        positive(cmd.cents)?;
        self.ledger.covers(cmd.cents)?;
        Ok(Withdrew { cents: cmd.cents })
    }

    /// A command whose refusals are typed objects (`refuses`): it returns
    /// its event, or one of the listed objects, and nothing else compiles.
    #[handles_cmd(Hold, refuses(InsufficientFunds, WalletClosed))]
    fn hold(&self, cmd: Hold, _ctx: &Context) -> Result<Held, Refuses<Self, Hold>> {
        if !self.open {
            return Err(WalletClosed {}.into());
        }
        match self.ledger.covers(cmd.cents) {
            Ok(()) => Ok(Held { cents: cmd.cents }),
            Err(_) => Err(InsufficientFunds {
                short: cmd.cents - self.ledger.balance(),
            }
            .into()),
        }
    }

    /// Any number of one event, a `Vec`, each decided from the state before
    /// any of them.
    #[handles_cmd(Sweep)]
    fn sweep(&self, cmd: Sweep, _ctx: &Context) -> Result<Vec<Withdrew>, Refused> {
        positive(cmd.chunk)?;
        Ok(self
            .ledger
            .pieces(cmd.chunk)
            .into_iter()
            .map(|cents| Withdrew { cents })
            .collect())
    }

    /// One event or none, an `Option`: closing a closed wallet is nothing.
    #[handles_cmd(Close)]
    fn close(&self, _cmd: Close, _ctx: &Context) -> Result<Option<Closed>, Refused> {
        match self.ledger.balance() {
            0 => Ok((!self.closed).then_some(Closed {})),
            cents => Err(Refused::NotEmpty { cents }),
        }
    }

    /// Records what the context says about the command: its stream, its id
    /// and its correlation.
    #[handles_cmd(Note)]
    fn note(&self, _cmd: Note, ctx: &Context) -> Result<Noted, Refused> {
        Ok(Noted {
            root: ctx
                .command
                .cover
                .clone()
                .unwrap_or_default()
                .root
                .unwrap_or_default()
                .value
                .to_vec(),
            command_id: ctx
                .command
                .command_id
                .clone()
                .unwrap_or_default()
                .value
                .to_vec(),
            correlation_id: ctx
                .command
                .correlation_id
                .clone()
                .unwrap_or_default()
                .value
                .to_vec(),
        })
    }

    /// A deliberate bug: a step that always continues, so the decision
    /// runs into the step limit.
    #[handles_cmd(Spin)]
    fn spin(&self, _cmd: Spin, _ctx: &Context) -> Result<Emit<Self>, Refused> {
        Ok(Emit::of(Spun {}).then(Self::spin_again))
    }

    fn spin_again(&self, _ctx: &Context) -> Result<Emit<Self>, Refused> {
        Ok(Emit::of(Spun {}).then(Self::spin_again))
    }

    /// A fact (sent by a policy): no `Result`. A payout the wallet cannot
    /// cover is not refused; its refusal is recorded.
    #[handles_fact(Payout)]
    fn payout(&self, fact: Payout, _ctx: &Context) -> Record<Self> {
        match self.ledger.covers(fact.cents) {
            Ok(()) => Record::of(PaidOut { cents: fact.cents }),
            Err(_) => Record::of(PayoutRefused {
                cents: fact.cents,
                short: fact.cents - self.ledger.balance(),
            }),
        }
    }

    /// A fact with a continuation: the gift is recorded, then whether it
    /// earns the reward is decided on the balance after it. Nothing in it
    /// can refuse: a gift over the limit is recorded all the same.
    #[handles_fact(Gift)]
    fn gift(&self, fact: Gift, ctx: &Context) -> Record<Self> {
        Record::of(Deposited {
            cents: fact.cents,
            at: ctx.recorded_at(),
        })
        .then(Self::reward_due)
    }

    /// A fact's continuation step: no `Result`.
    fn reward_due(&self, _ctx: &Context) -> Option<Rewarded> {
        let cents = self.ledger.balance();
        (cents >= REWARD_AT && !self.rewarded).then_some(Rewarded { cents: cents / 10 })
    }

    #[applies(Opened)]
    fn opened(&mut self, _event: Opened) {
        self.open = true;
    }

    #[applies(Deposited)]
    fn deposited(&mut self, event: Deposited) {
        self.ledger.credit(event.cents, event.at);
    }

    #[applies(Rewarded)]
    fn rewarded(&mut self, event: Rewarded) {
        self.rewarded = true;
        self.ledger.bonus(event.cents);
    }

    #[applies(Withdrew)]
    fn withdrew(&mut self, event: Withdrew) {
        self.ledger.debit(event.cents);
    }

    #[applies(Held)]
    fn held(&mut self, event: Held) {
        self.ledger.debit(event.cents);
    }

    #[applies(Closed)]
    fn closed(&mut self, _event: Closed) {
        self.closed = true;
    }

    #[applies(Noted)]
    fn noted(&mut self, _event: Noted) {}

    #[applies(Spun)]
    fn spun(&mut self, _event: Spun) {}

    #[applies(PaidOut)]
    fn paid_out(&mut self, event: PaidOut) {
        self.ledger.debit(event.cents);
    }

    #[applies(PayoutRefused)]
    fn payout_refused(&mut self, _event: PayoutRefused) {}
}

fn positive(cents: i64) -> Result<(), Refused> {
    match cents > 0 {
        true => Ok(()),
        false => Err(Refused::NotPositive { cents }),
    }
}

// ---------------------------------------------------------------- the lamp

/// The smallest aggregate: no snapshot form, and a rejection (inferred,
/// `Infallible`) it never returns.
#[derive(Clone, Default, PartialEq, Debug)]
pub struct Lamp {
    on: bool,
}

#[aggregate(domain = "lamp")]
impl Lamp {
    #[handles_cmd(SwitchOn)]
    fn switch_on(&self, _cmd: SwitchOn, _ctx: &Context) -> Result<Option<SwitchedOn>, Infallible> {
        Ok((!self.on).then_some(SwitchedOn {}))
    }

    #[applies(SwitchedOn)]
    fn switched_on(&mut self, _event: SwitchedOn) {
        self.on = true;
    }
}

// ---------------------------------------------------------------- the meter

domain_type!(Gauge { reading: i64 });
domain_type!(Gauged { reading: i64 });
domain_type!(Calibrate {});
domain_type!(Calibrated {});
domain_type!(Tune { reading: i64 });
domain_type!(Seal { code: i64 });
domain_type!(Sealed {});
domain_type!(MeterSealed {});
domain_type!(Probe {
    status: i64,
    step: bool,
});
domain_type!(Probed {});

/// The meter's typed refusal object.
impl fmt::Display for MeterSealed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the meter is sealed")
    }
}

impl Diagnostic for MeterSealed {
    fn code(&self) -> &str {
        "toy-meter-sealed"
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        vec![]
    }
}

/// The meter's bug: a defect, never a business answer, keyed and with its
/// args like any diagnostic.
#[derive(Clone, PartialEq, Debug)]
pub struct MeterBroke {
    seat: i64,
}

impl fmt::Display for MeterBroke {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "the meter at seat {} broke", self.seat)
    }
}

impl Diagnostic for MeterBroke {
    fn code(&self) -> &str {
        "toy-meter-broke"
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        vec![("seat", self.seat.into())]
    }
}

/// The meter's immediate return: its seat moved under the command, so the
/// caller re-reads and re-decides.
#[derive(Clone, PartialEq, Debug)]
pub struct MeterMoved {
    seat: i64,
}

impl fmt::Display for MeterMoved {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "the meter at seat {} moved", self.seat)
    }
}

impl Diagnostic for MeterMoved {
    fn code(&self) -> &str {
        "toy-meter-moved"
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        vec![("seat", self.seat.into())]
    }
}

/// The most a meter reads.
const METER_LIMIT: i64 = 1000;

std::thread_local! {
    /// The probe's outside dependency, as this test's thread sees it: how
    /// many more attempts it is down for (all of them, unless a test says
    /// otherwise), and how many attempts reached it. Outside the meter,
    /// as a dependency is: no event records it.
    static PROBE: std::cell::Cell<(u32, u32)> = const { std::cell::Cell::new((u32::MAX, 0)) };
}

/// Whether the probe's dependency is down for this attempt, counting it.
fn probe_down() -> bool {
    PROBE.with(|probe| {
        let (down, attempts) = probe.get();
        probe.set((down.saturating_sub(1), attempts + 1));
        down > 0
    })
}

/// The probe's dependency, down for the next `down` attempts.
fn probe_down_for(down: u32) {
    PROBE.with(|probe| probe.set((down, 0)));
}

/// How many attempts reached the probe's dependency.
fn probe_attempts() -> u32 {
    PROBE.with(|probe| probe.get().1)
}

/// The status a probe's `status` picks for its retryable bug: 1
/// UNAVAILABLE, 2 DEADLINE_EXCEEDED, 3 RESOURCE_EXHAUSTED; any other is
/// an immediate return.
fn probe_status(status: i64) -> Option<RetryableStatus> {
    match status {
        1 => Some(RetryableStatus::Unavailable),
        2 => Some(RetryableStatus::DeadlineExceeded),
        3 => Some(RetryableStatus::ResourceExhausted),
        _ => None,
    }
}

/// A probe's failure at `seat` while its dependency is down: a retryable
/// bug under the status `status` picks, else an immediate return.
fn probe_failure<X>(status: i64, seat: i64) -> Failed<X> {
    match probe_status(status) {
        Some(retryable) => Failed::retryable_bug(retryable, &MeterBroke { seat }),
        None => Failed::immediate(&MeterMoved { seat }),
    }
}

/// A handler that fails both ways, [`Failed`]: a refusal under a status of
/// the business set, a refusal by `?` from its own rejection, and a bug;
/// beside a handler that still returns its rejection as is. The rejection,
/// `Refused`, is inferred through `Failed`, and the two agree.
#[derive(Clone, Default, PartialEq, Debug)]
pub struct Meter {
    reading: i64,
}

#[aggregate(domain = "meter")]
impl Meter {
    /// A negative reading is the caller's mistake (INVALID_ARGUMENT); one
    /// over the limit refuses by `?` (FAILED_PRECONDITION); a zero reading
    /// is the meter's bug.
    #[handles_cmd(Gauge)]
    fn gauge(&self, cmd: Gauge, _ctx: &Context) -> Result<Gauged, Failed<Refused>> {
        if cmd.reading == 0 {
            return Err(Failed::bug(&MeterBroke { seat: 3 }));
        }
        if cmd.reading < 0 {
            return Err(Failed::from(Refused::NotPositive { cents: cmd.reading })
                .with_status(BusinessStatus::InvalidArgument));
        }
        within(cmd.reading)?;
        Ok(Gauged {
            reading: cmd.reading,
        })
    }

    /// A reading, then a step that fails both ways: a zero reading is the
    /// step's bug, a negative one its refusal (INVALID_ARGUMENT); either
    /// fails the whole decision.
    #[handles_cmd(Tune)]
    fn tune(&self, cmd: Tune, _ctx: &Context) -> Result<Emit<Self>, Refused> {
        let reading = cmd.reading;
        Ok(Emit::of(Gauged { reading: 1 })
            .then(move |meter: &Self, ctx: &Context| meter.adjust(reading, ctx)))
    }

    /// A typed-refusal handler's two ways to fail: a zero code is its bug,
    /// a negative one its typed refusal under NOT_FOUND.
    /// A code over 100 is the probe's failure (a retryable bug or an
    /// immediate return, [`probe_failure`]) of the code less 100.
    #[handles_cmd(Seal, refuses(MeterSealed))]
    fn seal(&self, cmd: Seal, _ctx: &Context) -> Result<Sealed, Failed<Refuses<Self, Seal>>> {
        if cmd.code == 0 {
            return Err(Failed::bug(&MeterBroke { seat: 9 }));
        }
        if cmd.code > 100 && probe_down() {
            return Err(probe_failure(cmd.code - 100, 9));
        }
        if cmd.code < 0 {
            return Err(Failed::rejected(MeterSealed {}).with_status(BusinessStatus::NotFound));
        }
        Ok(Sealed {})
    }

    /// The handler as it was before `Failed`: its own rejection, as is.
    #[handles_cmd(Calibrate)]
    fn calibrate(&self, _cmd: Calibrate, _ctx: &Context) -> Result<Calibrated, Refused> {
        match self.reading {
            0 => Ok(Calibrated {}),
            cents => Err(Refused::NotEmpty { cents }),
        }
    }

    /// A probe of an outside dependency: while it is down, the handler
    /// (or, with `step`, its continuation step) fails as `status` picks
    /// ([`probe_failure`]); once it is up, a `Probed`.
    #[handles_cmd(Probe)]
    fn probe(&self, cmd: Probe, _ctx: &Context) -> Result<Emit<Self>, Failed<Refused>> {
        let status = cmd.status;
        if !cmd.step {
            return match probe_down() {
                true => Err(probe_failure(status, 5)),
                false => Ok(Emit::of(Probed {})),
            };
        }
        Ok(
            Emit::of(Probed {}).then(move |_: &Self, _: &Context| match probe_down() {
                true => Err(probe_failure::<Refused>(status, 6)),
                false => Ok(None::<Probed>),
            }),
        )
    }

    #[applies(Probed)]
    fn probed(&mut self, _event: Probed) {}

    #[applies(Gauged)]
    fn gauged(&mut self, event: Gauged) {
        self.reading = event.reading;
    }

    #[applies(Sealed)]
    fn sealed(&mut self, _event: Sealed) {}

    #[applies(Calibrated)]
    fn calibrated(&mut self, _event: Calibrated) {
        self.reading = 0;
    }
}

impl Meter {
    /// [`Meter::tune`]'s step.
    fn adjust(&self, reading: i64, _ctx: &Context) -> Result<Option<Gauged>, Failed<Refused>> {
        if reading == 0 {
            return Err(Failed::bug(&MeterBroke { seat: self.reading }));
        }
        if reading < 0 {
            return Err(Failed::from(Refused::NotPositive { cents: reading })
                .with_status(BusinessStatus::InvalidArgument));
        }
        Ok(Some(Gauged { reading }))
    }
}

fn within(reading: i64) -> Result<(), Refused> {
    match reading > METER_LIMIT {
        true => Err(Refused::OverLimit {
            cents: reading,
            limit: METER_LIMIT,
        }),
        false => Ok(()),
    }
}

// ---------------------------------------------------------------- the ladder

domain_type!(Climb { to: u32 });
domain_type!(Slip {});
domain_type!(Rung {});

/// A decision that climbs one rung per step, to the step limit's edge
/// and past it; and a fact whose steps never stop.
#[derive(Clone, Default, PartialEq, Debug)]
pub struct Ladder {
    rungs: u32,
    to: u32,
}

#[aggregate(domain = "ladder")]
impl Ladder {
    #[handles_cmd(Climb)]
    fn climb(&self, cmd: Climb, _ctx: &Context) -> Result<Emit<Self>, Infallible> {
        let to = cmd.to;
        Ok(Emit::of(Rung {}).then(move |ladder: &Ladder, ctx: &Context| ladder.higher(to, ctx)))
    }

    /// One more rung while below `to`.
    fn higher(&self, to: u32, _ctx: &Context) -> Result<Emit<Self>, Infallible> {
        match self.rungs < to {
            true => Ok(Emit::of(Rung {})
                .then(move |ladder: &Ladder, ctx: &Context| ladder.higher(to, ctx))),
            false => Ok(Emit::of(None::<Rung>)),
        }
    }

    #[handles_fact(Slip)]
    fn slip(&self, _fact: Slip, _ctx: &Context) -> Record<Self> {
        Record::of(Rung {}).then(Self::slipping)
    }

    fn slipping(&self, _ctx: &Context) -> Record<Self> {
        Record::of(Rung {}).then(Self::slipping)
    }

    #[applies(Rung)]
    fn rung(&mut self, _event: Rung) {
        self.rungs += 1;
        self.to = self.to.max(self.rungs);
    }
}

// ---------------------------------------------------------------- the pot

/// An aggregate whose handlers read angzarr-router's whole
/// `CommandContext`, their [`Context`]'s `command`: a stake decided under
/// AGGREGATE_HANDLES reads the window it did not see and, when told to,
/// acknowledges it; a mark records the stream's root; a peek records what
/// the router knows of the input.
#[derive(Clone, Default, PartialEq, Debug)]
pub struct Pot {
    cents: i64,
}

#[aggregate(domain = "pot")]
impl Pot {
    /// A stake that does not acknowledge never reads the context, so the
    /// router finds the window unacknowledged.
    #[handles_cmd(Stake)]
    fn stake(&self, cmd: Stake, ctx: &Context) -> Result<Staked, Infallible> {
        if !cmd.acknowledge {
            return Ok(Staked {
                cents: cmd.cents,
                window: 0,
            });
        }
        let window = match &ctx.command.conflict {
            Some(conflict) => {
                conflict.resolve_conflicts(conflict.window(), conflict.nonce());
                conflict.window().len() as u32
            }
            None => 0,
        };
        Ok(Staked {
            cents: cmd.cents,
            window,
        })
    }

    /// The stream's root and domain, from the router's cover.
    #[handles_cmd(Mark)]
    fn mark(&self, _cmd: Mark, ctx: &Context) -> Result<Marked, Infallible> {
        let cover = ctx.command.cover.clone().unwrap_or_default();
        Ok(Marked {
            root: cover.root.unwrap_or_default().value.to_vec(),
            domain: cover.domain,
        })
    }

    /// What the router knows of the input, under whatever resolution it
    /// was decided.
    #[handles_cmd(Peek)]
    fn peek(&self, _cmd: Peek, ctx: &Context) -> Result<Peeked, Infallible> {
        Ok(Peeked {
            next_sequence: ctx.command.next_sequence,
            had_prior_events: ctx.command.had_prior_events,
            expected_sequence: ctx.command.expected_sequence,
            resolution: ctx.command.conflict_resolution as i32,
            conflict: ctx.command.conflict.is_some(),
        })
    }

    #[applies(Staked)]
    fn staked(&mut self, event: Staked) {
        self.cents += event.cents;
    }

    #[applies(Marked)]
    fn marked(&mut self, _event: Marked) {}

    #[applies(Peeked)]
    fn peeked(&mut self, _event: Peeked) {}
}

// ---------------------------------------------------------------- the tally, by hand

wire::message!(
    "toy.tally.v1",
    TallyState {
        #[prost(uint64, tag = "1")]
        count: u64,
    }
);
wire::message!("toy.tally.cmd.v1", Count {});
wire::message!("toy.tally.v1", Counted {});

impl State for TallyState {
    type Snapshot = Self;

    fn to_snapshot(&self) -> Self {
        self.clone()
    }

    fn from_snapshot(snapshot: Self) -> Result<Self, Report> {
        Ok(snapshot)
    }

    fn descriptor() -> StateDescriptor {
        StateDescriptor {
            type_url: "/toy.tally.v1.TallyState".to_string(),
            options: Some(StateOptions {
                conflict_resolutions: vec![
                    ConflictResolution::Strict as i32,
                    ConflictResolution::AggregateHandles as i32,
                ],
            }),
        }
    }
}

impl Payload for Count {
    fn to_any(&self) -> Any {
        pack(self)
    }

    fn from_any(any: &Any) -> Result<Self, PayloadError> {
        Decoder::new().on::<Count>(|c| c).decode(any)
    }

    fn type_urls() -> Vec<String> {
        Decoder::new().on::<Count>(|c| c).type_urls()
    }
}

impl Payload for Counted {
    fn to_any(&self) -> Any {
        pack(self)
    }

    fn from_any(any: &Any) -> Result<Self, PayloadError> {
        Decoder::new().on::<Counted>(|c| c).decode(any)
    }

    fn type_urls() -> Vec<String> {
        Decoder::new().on::<Counted>(|c| c).type_urls()
    }
}

/// A domain whose `Domain` impl is written by hand, as any domain's may
/// be.
pub struct Tally;

impl Domain for Tally {
    type State = TallyState;
    type Command = Count;
    type Fact = Infallible;
    type Event = Counted;
    type Rejection = Infallible;
    const RULES_VERSION: &'static str = "v1";

    const NAME: &'static str = "tally";

    fn apply(state: &mut TallyState, _: &Counted) {
        state.count += 1;
    }
}

/// The tally's handler, written by hand as angzarr-router's table: every
/// count counts.
fn tally_handlers(
    table: ::angzarr_embedded::next::angzarr_router::domain::DomainDispatch<TallyState>,
) -> ::angzarr_embedded::next::angzarr_router::domain::DomainDispatch<TallyState> {
    table.on_command("toy.tally.cmd.v1.Count", |_, _, _| {
        Ok(Some(EventBook {
            pages: vec![EventPage {
                payload: Some(Counted {}.to_any()),
                ..Default::default()
            }],
            ..Default::default()
        }))
    })
}

// ---------------------------------------------------------------- running them

/// The marked-up aggregates, each registered with its factory, and the
/// hand-written tally, registered as before.
fn registry() -> Registry {
    configured(WalletConfig::default())
}

/// As [`registry`], with every wallet built with `config`.
fn configured(config: WalletConfig) -> Registry {
    let mut registry = Registry::new();
    registry
        .aggregate::<Wallet>(move || Wallet::with(config.clone()))
        .aggregate::<Lamp>(Lamp::default)
        .aggregate::<Meter>(Meter::default)
        .aggregate::<Ladder>(Ladder::default)
        .aggregate::<Pot>(Pot::default);
    registry
        .register::<Tally>(Upcaster::new("tally-upcaster", "tally"), tally_handlers)
        .unwrap();
    registry
}

fn root() -> Uuid {
    Uuid::from_bytes([0x0a; 16])
}

/// A domain value as the `Any` it travels as.
fn any<M: DomainMessage>(message: M) -> Any {
    pack(&message.to_wire())
}

fn at(seconds: i64) -> Timestamp {
    Timestamp { seconds, nanos: 0 }
}

/// Command `n` to `domain`'s stream, as a client sends it, with
/// `correlation` if given.
fn send(domain: &str, n: u8, payload: Any, correlation: Option<Uuid>) -> CommandBook {
    CommandBook {
        cover: Some(Cover {
            domain: domain.to_string(),
            root: Some(root()),
            edition: None,
        }),
        page: Some(CommandPage {
            body: Some(command_page::Body::Command(Command {
                payload: Some(payload),
            })),
            command_id: Some(Uuid::from_bytes([n; 16])),
            correlation_id: correlation,
            ..CommandPage::default()
        }),
        handling: None,
    }
}

/// Execute `book` (command `n`, sampled at `n` seconds) on `stream`: the
/// events it committed, as the `Any`s they are stored as, or why it was
/// refused.
fn execute<D: Domain>(
    stream: &mut Stream<D>,
    registry: &Registry,
    n: u8,
    book: CommandBook,
) -> Result<Vec<Any>, Failure<Rejected>> {
    let sampled = Sampled {
        now: at(i64::from(n)),
        command_id: Uuid::from_bytes([0x07; 16]),
    };
    let log = MemoryDeadLetters::new();
    match stream
        .execute(book, registry, &sampled, &log, &mut [])
        .ready()?
    {
        Executed::Applied(events) => Ok(events.iter().map(|e| e.event().to_any()).collect()),
        Executed::Duplicate(_) => panic!("command {n} is new"),
    }
}

/// A fact to `domain`'s stream from `external_id`, with no ids: the kernel
/// derives them.
fn fact(domain: &str, payload: Any, external_id: &str) -> CommandBook {
    CommandBook {
        cover: Some(Cover {
            domain: domain.to_string(),
            root: Some(root()),
            edition: None,
        }),
        page: Some(CommandPage {
            body: Some(command_page::Body::Fact(Fact {
                payload: Some(payload),
            })),
            fact_origin: Some(FactOrigin {
                external_id: external_id.to_string(),
                description: "toy payout".to_string(),
            }),
            ..CommandPage::default()
        }),
        handling: None,
    }
}

/// Run command `n` on `stream`, as [`execute`], with no correlation given.
fn run<D: Domain>(
    stream: &mut Stream<D>,
    registry: &Registry,
    n: u8,
    payload: Any,
) -> Result<Vec<Any>, Failure<Rejected>> {
    execute(stream, registry, n, send(D::NAME, n, payload, None))
}

fn wallet() -> Stream<Wallet> {
    Stream::new(root(), None).unwrap()
}

/// A wallet opened with `bonus` by command 1.
fn opened(registry: &Registry, bonus: i64) -> Stream<Wallet> {
    let mut wallet = wallet();
    run(&mut wallet, registry, 1, any(Open { bonus })).unwrap();
    wallet
}

/// The refusal `failure` is, as its caller receives it: its code and its
/// English.
fn rejection(failure: Failure<Rejected>) -> (String, Vec<(String, String)>) {
    match failure {
        Failure::Rejected(rejected) => said(&rejected),
        other => panic!("refused by the domain, not {other:?}"),
    }
}

/// What `diagnostic` says: its code and its args, each as text, by name
/// (decision 14.37: a key and a map, never its English). An arg the
/// router carried is text; the handler's own may be a number.
fn said(diagnostic: &dyn Diagnostic) -> (String, Vec<(String, String)>) {
    let args = diagnostic
        .args()
        .into_iter()
        .map(|(name, arg)| {
            let text = match arg {
                Arg::Int(n) => n.to_string(),
                Arg::Uint(n) => n.to_string(),
                Arg::Str(s) => s,
                Arg::Nested(cause) => cause.code().to_string(),
            };
            (name.to_string(), text)
        })
        .collect();
    (diagnostic.code().to_string(), args)
}

/// The wallet as its fields would be: open, not closed, not rewarded,
/// holding `cents`, last moved at `moved` seconds, configured by default.
fn holding(cents: i64, moved: i64) -> Wallet {
    Wallet {
        open: true,
        closed: false,
        rewarded: false,
        ledger: Ledger {
            cents,
            last_moved_at: Some(at(moved)),
        },
        config: WalletConfig::default(),
    }
}

fn context(n: u8) -> Context {
    Context::new(at(i64::from(n))).with_command(CommandContext {
        cover: Some(wallet().cover().clone()),
        command_id: Some(Uuid::from_bytes([n; 16])),
        correlation_id: Some(Uuid::from_bytes([n; 16])),
        recorded_at: Some(at(i64::from(n))),
        ..CommandContext::default()
    })
}

mod context_instant {
    use super::*;

    /// A handler's instant is the router's, `ctx.command.recorded_at`: the
    /// `ContextualCommand.recorded_at` the kernel stamped (decision 14.37).
    /// The context keeps no instant of its own beside it, so the two never
    /// disagree.
    #[test]
    fn a_contexts_instant_is_the_routers() {
        let ctx = Context::new(at(5));
        assert_eq!(
            ctx.command.recorded_at,
            Some(at(5)),
            "a unit test's instant is the router's field"
        );
        let ctx = ctx.with_command(CommandContext {
            recorded_at: Some(at(9)),
            ..CommandContext::default()
        });
        assert_eq!(ctx.recorded_at(), at(9));
    }
}

mod route {
    use super::*;

    /// Each command, sent as the `Any` a client sends, reaches the handler
    /// its type URL names, as a domain value.
    #[test]
    fn each_type_url_goes_to_its_handler() {
        let registry = registry();
        let mut wallet = wallet();

        let open = run(&mut wallet, &registry, 1, any(Open { bonus: 5 }));
        let deposit = run(&mut wallet, &registry, 2, any(Deposit { cents: 50 }));
        let withdraw = run(&mut wallet, &registry, 3, any(Withdraw { cents: 20 }));

        assert_eq!(
            open,
            Ok(vec![
                any(Opened {}),
                any(Deposited {
                    cents: 5,
                    at: at(1)
                })
            ])
        );
        assert_eq!(
            deposit,
            Ok(vec![any(Deposited {
                cents: 50,
                at: at(2)
            })])
        );
        assert_eq!(withdraw, Ok(vec![any(Withdrew { cents: 20 })]));
    }

    /// A type URL no handler is marked for (a command the proto declares
    /// and the markup does not handle, or another domain's) is the router's
    /// ROUTER_REASON_UNKNOWN_COMMAND_TYPE, not a rejection, and reaches no handler.
    #[test]
    fn an_unmarked_type_url_is_the_routers_unknown_command_type() {
        let registry = registry();
        let mut wallet = opened(&registry, 0);

        let refused = run(&mut wallet, &registry, 2, pack(&Count {}));

        match refused {
            Err(Failure::Kernel(KernelError::Router(e))) => {
                assert_eq!(e.code, "ROUTER_REASON_UNKNOWN_COMMAND_TYPE");
                assert_eq!(
                    e.extras.get("type_url").map(String::as_str),
                    Some("/toy.tally.cmd.v1.Count")
                );
            }
            other => panic!("the router's ROUTER_REASON_UNKNOWN_COMMAND_TYPE, not {other:?}"),
        }
        assert_eq!(wallet.len(), 2, "nothing was committed");
    }

    /// The generated command and event sets read exactly the marked types'
    /// type URLs: what an upcast chain must end at.
    #[test]
    fn the_payload_sets_are_the_marked_types() {
        let mut commands = <<Wallet as Domain>::Command as Payload>::type_urls();
        let mut facts = <<Wallet as Domain>::Fact as Payload>::type_urls();
        let mut events = <<Wallet as Domain>::Event as Payload>::type_urls();
        commands.sort();
        facts.sort();
        events.sort();

        assert_eq!(
            commands,
            [
                "/toy.wallet.cmd.v1.Close",
                "/toy.wallet.cmd.v1.Deposit",
                "/toy.wallet.cmd.v1.Hold",
                "/toy.wallet.cmd.v1.Note",
                "/toy.wallet.cmd.v1.Open",
                "/toy.wallet.cmd.v1.Spin",
                "/toy.wallet.cmd.v1.Sweep",
                "/toy.wallet.cmd.v1.Withdraw",
            ]
        );
        assert_eq!(
            facts,
            ["/toy.wallet.fact.v1.Gift", "/toy.wallet.fact.v1.Payout"]
        );
        assert_eq!(
            events,
            [
                "/toy.wallet.v1.Closed",
                "/toy.wallet.v1.Deposited",
                "/toy.wallet.v1.Held",
                "/toy.wallet.v1.Noted",
                "/toy.wallet.v1.Opened",
                "/toy.wallet.v1.PaidOut",
                "/toy.wallet.v1.PayoutRefused",
                "/toy.wallet.v1.Rewarded",
                "/toy.wallet.v1.Spun",
                "/toy.wallet.v1.Withdrew",
            ]
        );
    }
}

mod emit {
    use super::*;

    /// A handler returns its event; the router applies it through the
    /// applier, so the next command sees it.
    #[test]
    fn a_returned_event_is_applied_by_the_router() {
        let registry = registry();
        let mut wallet = opened(&registry, 5);

        run(&mut wallet, &registry, 2, any(Deposit { cents: 50 })).unwrap();

        assert_eq!(wallet.state(), &holding(55, 2));
    }

    /// A tuple is recorded and applied in its order.
    #[test]
    fn a_tuple_is_applied_and_recorded_in_order() {
        let registry = registry();
        let mut wallet = wallet();

        let events = run(&mut wallet, &registry, 1, any(Open { bonus: 5 }));

        assert_eq!(
            events,
            Ok(vec![
                any(Opened {}),
                any(Deposited {
                    cents: 5,
                    at: at(1)
                })
            ])
        );
        assert_eq!(wallet.state(), &holding(5, 1));
    }

    /// A `Vec` is recorded and applied in its order, each event decided
    /// from the state before the command.
    #[test]
    fn a_vec_is_applied_and_recorded_in_order() {
        let registry = registry();
        let mut wallet = opened(&registry, 25);

        let events = run(&mut wallet, &registry, 2, any(Sweep { chunk: 10 }));

        assert_eq!(
            events,
            Ok(vec![
                any(Withdrew { cents: 10 }),
                any(Withdrew { cents: 10 }),
                any(Withdrew { cents: 5 }),
            ])
        );
        assert_eq!(wallet.state(), &holding(0, 1));
    }

    /// `Some` is one event; `None` is none, and commits nothing.
    #[test]
    fn an_option_is_one_event_or_none() {
        let registry = registry();
        let mut wallet = opened(&registry, 0);

        let first = run(&mut wallet, &registry, 2, any(Close {}));
        let again = run(&mut wallet, &registry, 3, any(Close {}));

        assert_eq!(first, Ok(vec![any(Closed {})]));
        assert_eq!(again, Ok(vec![]));
        assert_eq!(wallet.len(), 3, "the second close committed nothing");
        assert!(wallet.state().closed);
    }
}

mod context {
    use super::*;

    /// The handler's instant is the context's: the recorded instant the
    /// application sampled, not a clock.
    #[test]
    fn the_recorded_instant_reaches_the_handler() {
        let registry = registry();
        let mut wallet = opened(&registry, 0);

        let events = run(&mut wallet, &registry, 7, any(Deposit { cents: 1 }));

        assert_eq!(
            events,
            Ok(vec![any(Deposited {
                cents: 1,
                at: at(7)
            })])
        );
        assert_eq!(wallet.state().ledger.last_moved_at, Some(at(7)));
    }

    /// The stream, the command's id and its correlation (as sent) reach
    /// the handler.
    #[test]
    fn the_stream_command_id_and_correlation_reach_the_handler() {
        let registry = registry();
        let mut wallet = opened(&registry, 0);
        let correlation = Uuid::from_bytes([0x0c; 16]);

        let noted = execute(
            &mut wallet,
            &registry,
            2,
            send("wallet", 2, any(Note {}), Some(correlation.clone())),
        );

        assert_eq!(
            noted,
            Ok(vec![any(Noted {
                root: root().value.to_vec(),
                command_id: Uuid::from_bytes([2; 16]).value.to_vec(),
                correlation_id: correlation.value.to_vec(),
            })])
        );
    }

    /// With no correlation sent, the context's is the command's id.
    #[test]
    fn the_correlation_defaults_to_the_command_id() {
        let registry = registry();
        let mut wallet = opened(&registry, 0);

        let noted = run(&mut wallet, &registry, 2, any(Note {}));

        assert_eq!(
            noted,
            Ok(vec![any(Noted {
                root: root().value.to_vec(),
                command_id: Uuid::from_bytes([2; 16]).value.to_vec(),
                correlation_id: Uuid::from_bytes([2; 16]).value.to_vec(),
            })])
        );
    }
}

mod continuation {
    use super::*;

    /// The step runs on the wallet with the deposit applied: 120 deposited
    /// is at the reward's threshold only once it is in the balance, and
    /// both events are one command's.
    #[test]
    fn a_step_sees_the_state_after_the_events_before_it() {
        let registry = registry();
        let mut wallet = opened(&registry, 0);

        let events = run(&mut wallet, &registry, 2, any(Deposit { cents: 120 }));
        let again = run(&mut wallet, &registry, 3, any(Deposit { cents: 10 }));

        assert_eq!(
            events,
            Ok(vec![
                any(Deposited {
                    cents: 120,
                    at: at(2)
                }),
                any(Rewarded { cents: 12 }),
            ])
        );
        assert_eq!(
            again,
            Ok(vec![any(Deposited {
                cents: 10,
                at: at(3)
            })]),
            "rewarded once"
        );
        assert_eq!(wallet.len(), 5);
        assert_eq!(wallet.state().ledger.balance(), 142);
        assert!(wallet.state().rewarded);
    }

    /// A step's rejection refuses the whole decision: the deposit the
    /// handler returned is not committed either.
    #[test]
    fn a_rejection_in_a_step_refuses_the_whole_decision() {
        let registry = registry();
        let mut wallet = opened(&registry, 5);
        let before = wallet.state().clone();

        let refused = run(&mut wallet, &registry, 2, any(Deposit { cents: 1500 }));

        assert_eq!(
            refused.map_err(rejection),
            Err(said(&Refused::OverLimit {
                cents: 1505,
                limit: 1000
            }))
        );
        assert_eq!(wallet.len(), 2, "nothing was committed");
        assert_eq!(wallet.state(), &before, "nothing was applied");
    }

    /// A decision whose steps never stop is the kernel's `TooManySteps`,
    /// at the limit, with nothing committed.
    #[test]
    fn steps_past_the_limit_are_too_many() {
        let registry = registry();
        let mut wallet = opened(&registry, 0);

        let refused = run(&mut wallet, &registry, 2, any(Spin {}));

        match refused {
            Err(Failure::Kernel(KernelError::TooManySteps { stream, limit })) => {
                assert_eq!(limit, STEP_LIMIT);
                assert_eq!(&stream, wallet.cover());
            }
            other => panic!("too many steps, not {other:?}"),
        }
        assert_eq!(wallet.len(), 2, "nothing was committed");
    }

    /// A decision that runs exactly the limit's steps (the last emitting
    /// nothing) commits; one more step is too many.
    #[test]
    fn the_limit_is_the_last_step_allowed() {
        let registry = registry();
        let mut ladder = Stream::<Ladder>::new(root(), None).unwrap();
        let climbed = run(
            &mut ladder,
            &registry,
            1,
            any(Climb {
                to: STEP_LIMIT as u32,
            }),
        );
        assert_eq!(climbed.map(|events| events.len()), Ok(STEP_LIMIT));
        let mut ladder = Stream::<Ladder>::new(root(), None).unwrap();
        let too_far = run(
            &mut ladder,
            &registry,
            1,
            any(Climb {
                to: STEP_LIMIT as u32 + 1,
            }),
        );
        assert!(
            matches!(
                too_far,
                Err(Failure::Kernel(KernelError::TooManySteps { .. }))
            ),
            "{too_far:?}"
        );
    }

    /// A fact's steps that never stop are a bug: the ingest panics, which
    /// the router catches (its cause ROUTER_REASON_HANDLER_PANICKED) and the kernel
    /// dead-letters.
    #[test]
    fn a_facts_steps_past_the_limit_are_a_bug() {
        let registry = registry();
        let mut ladder = Stream::<Ladder>::new(root(), None).unwrap();
        let slipped = execute(
            &mut ladder,
            &registry,
            1,
            fact("ladder", any(Slip {}), "slip-1"),
        );
        assert!(
            matches!(
                &slipped,
                Err(Failure::Kernel(KernelError::Router(e)))
                    if e.extras.get("cause").map(String::as_str) == Some("ROUTER_REASON_HANDLER_PANICKED")
            ),
            "{slipped:?}"
        );
        assert_eq!(slipped.err().map(|f| f.recovery()), Some(Recovery::Bug));
        assert!(ladder.is_empty());
    }

    /// The helper runs the step, applies what it emits, and finds that
    /// re-running it there emits nothing; it returns the instance after
    /// the first run.
    #[test]
    fn the_helper_passes_an_idempotent_step() {
        let mut unrewarded = holding(150, 1);
        unrewarded.rewarded = false;

        let after = assert_step_idempotent(unrewarded, Wallet::settle, &context(2));

        assert!(after.rewarded);
        assert_eq!(after.ledger.balance(), 165);
    }

    /// A step that tips on every run is not idempotent.
    #[test]
    #[should_panic(expected = "not idempotent")]
    fn the_helper_names_a_step_that_emits_again() {
        fn always_tips(_wallet: &Wallet, _ctx: &Context) -> Result<Rewarded, Refused> {
            Ok(Rewarded { cents: 1 })
        }

        assert_step_idempotent(holding(150, 1), always_tips, &context(2));
    }
}

mod construction {
    use super::*;

    /// Replay starts from the registered factory, not `Default`: the
    /// rebuilt wallet carries the factory's configuration, and its handlers
    /// and steps decide under it.
    #[test]
    fn replay_starts_from_the_registered_factory() {
        let mut original = opened(&registry(), 0);
        run(&mut original, &registry(), 2, any(Deposit { cents: 50 })).unwrap();
        let pages: Vec<_> = original.events().iter().map(Envelope::page).collect();
        let registry = configured(WalletConfig { limit: 60 });

        let mut rebuilt =
            Stream::<Wallet>::replay(original.cover().clone(), &pages, &registry).unwrap();
        let refused = run(&mut rebuilt, &registry, 3, any(Deposit { cents: 20 }));

        assert_eq!(rebuilt.state().config, WalletConfig { limit: 60 });
        assert_eq!(
            refused.map_err(rejection),
            Err(said(&Refused::OverLimit {
                cents: 70,
                limit: 60
            }))
        );
    }

    /// Resuming from a snapshot builds from the factory, then restores: the
    /// configuration, which no snapshot holds, survives.
    #[test]
    fn a_resumed_aggregate_keeps_its_factorys_configuration() {
        let registry = configured(WalletConfig { limit: 60 });
        let mut wallet =
            Stream::<Wallet>::replay(wallet().cover().clone(), &[], &registry).unwrap();
        run(&mut wallet, &registry, 1, any(Open { bonus: 0 })).unwrap();
        run(&mut wallet, &registry, 2, any(Deposit { cents: 50 })).unwrap();
        let snapshot = wallet.snapshot(&registry).expect("the wallet snapshots");

        let mut resumed = Stream::<Wallet>::resume(
            wallet.cover().clone(),
            &snapshot,
            wallet.last_recorded_at(),
            &[],
            &registry,
        )
        .unwrap();
        let refused = run(&mut resumed, &registry, 3, any(Deposit { cents: 20 }));

        assert_eq!(resumed.state(), wallet.state());
        assert_eq!(resumed.state().config, WalletConfig { limit: 60 });
        assert_eq!(
            refused.map_err(rejection),
            Err(said(&Refused::OverLimit {
                cents: 70,
                limit: 60
            }))
        );
    }
}

mod facts {
    use super::*;

    /// A fact's handler records its outcome either way: paid out when the
    /// wallet covers it, a recorded refusal when it does not.
    #[test]
    fn a_fact_handler_records_its_outcome() {
        let registry = registry();
        let mut wallet = opened(&registry, 10);

        let paid = execute(
            &mut wallet,
            &registry,
            2,
            fact("wallet", any(Payout { cents: 4 }), "payout-1"),
        );
        let refused = execute(
            &mut wallet,
            &registry,
            3,
            fact("wallet", any(Payout { cents: 30 }), "payout-2"),
        );

        assert_eq!(paid, Ok(vec![any(PaidOut { cents: 4 })]));
        assert_eq!(
            refused,
            Ok(vec![any(PayoutRefused {
                cents: 30,
                short: 24
            })])
        );
        assert_eq!(wallet.len(), 4, "both outcomes were committed");
        assert_eq!(wallet.state().ledger.balance(), 6);
    }

    /// Where a command is refused, a fact is not: the same amount from the
    /// same wallet is a rejection as a command and a recorded refusal as a
    /// fact.
    #[test]
    fn a_fact_is_recorded_where_a_command_is_refused() {
        let registry = registry();
        let mut wallet = opened(&registry, 10);

        let command = run(&mut wallet, &registry, 2, any(Withdraw { cents: 30 }));
        let recorded = execute(
            &mut wallet,
            &registry,
            3,
            fact("wallet", any(Payout { cents: 30 }), "payout-3"),
        );

        assert_eq!(
            command.map_err(rejection),
            Err(said(&Refused::Overdrawn { short: 20 }))
        );
        assert_eq!(
            recorded,
            Ok(vec![any(PayoutRefused {
                cents: 30,
                short: 20
            })])
        );
        assert_eq!(wallet.len(), 3);
    }
}

mod fact_continuations {
    use super::*;

    /// A fact's continuation runs on the state after its events, as a
    /// command's does, and cannot refuse: the gift that brings the wallet
    /// to the reward earns it, and a gift past the limit (which a deposit
    /// command's step refuses) is recorded all the same.
    #[test]
    fn a_facts_continuation_runs_after_its_events_and_cannot_refuse() {
        let registry = configured(WalletConfig { limit: 150 });
        let mut wallet = opened(&registry, 10);

        let rewarded = execute(
            &mut wallet,
            &registry,
            2,
            fact("wallet", any(Gift { cents: 95 }), "gift-1"),
        );
        let past_the_limit = execute(
            &mut wallet,
            &registry,
            3,
            fact("wallet", any(Gift { cents: 500 }), "gift-2"),
        );

        assert_eq!(
            rewarded,
            Ok(vec![
                any(Deposited {
                    cents: 95,
                    at: at(2)
                }),
                any(Rewarded { cents: 10 })
            ])
        );
        assert_eq!(
            past_the_limit,
            Ok(vec![any(Deposited {
                cents: 500,
                at: at(3)
            })]),
            "rewarded once; and recorded, never refused"
        );
    }
}

mod registration {
    use super::*;

    /// A marked-up aggregate's rules version is its crate's build version
    /// (`CARGO_PKG_VERSION`, the version angzarr-build writes into its
    /// descriptor), and its events are recorded under it.
    #[test]
    fn its_events_carry_its_crates_build_version() {
        let registry = registry();
        assert_eq!(
            registry.rules_version("wallet").as_deref(),
            Some(env!("CARGO_PKG_VERSION"))
        );
        let wallet = opened(&registry, 5);
        for envelope in wallet.events() {
            assert_eq!(envelope.rules_version(), env!("CARGO_PKG_VERSION"));
        }
    }

    /// An upcaster given at registration reads the aggregate's old events:
    /// an old `Deposited` without its instant replays as one at second 0.
    #[test]
    fn an_upcaster_given_reads_its_old_events() {
        let mut registry = Registry::new();
        registry.aggregate::<Wallet>(Wallet::default).upcaster(
            Upcaster::new("wallet-upcaster", "wallet").rule(
                |old: wire::OldDeposited| -> Result<wire::Deposited, Report> {
                    Ok(wire::Deposited {
                        cents: old.cents,
                        at: Some(at(0)),
                    })
                },
            ),
        );
        let original = opened(&registry, 7);
        let mut pages: Vec<_> = original.events().iter().map(Envelope::page).collect();
        pages[1].payload = Some(pack(&wire::OldDeposited { cents: 7 }));

        let replayed =
            Stream::<Wallet>::replay(original.cover().clone(), &pages, &registry).unwrap();

        assert_eq!(
            replayed.state().ledger,
            Ledger {
                cents: 7,
                last_moved_at: Some(at(0)),
            }
        );
    }

    /// Registering one aggregate twice is a bug in the composition root:
    /// it panics there, at startup, rather than being an error to handle.
    #[test]
    #[should_panic(expected = "wallet")]
    fn registering_an_aggregate_twice_panics() {
        let mut registry = Registry::new();
        registry
            .aggregate::<Wallet>(Wallet::default)
            .aggregate::<Wallet>(Wallet::default);
    }
}

mod replay {
    use super::*;

    /// A stream rebuilt from its stored pages is a fresh instance with the
    /// events replayed through the appliers, and the next handler decides
    /// against it.
    #[test]
    fn a_fresh_instance_is_rebuilt_from_the_log_then_handles() {
        let registry = registry();
        let mut original = opened(&registry, 0);
        run(&mut original, &registry, 2, any(Deposit { cents: 50 })).unwrap();
        let pages: Vec<_> = original.events().iter().map(Envelope::page).collect();

        let mut rebuilt =
            Stream::<Wallet>::replay(original.cover().clone(), &pages, &registry).unwrap();
        let refused = run(&mut rebuilt, &registry, 3, any(Withdraw { cents: 60 }));
        let withdrew = run(&mut rebuilt, &registry, 4, any(Withdraw { cents: 20 }));

        assert_eq!(
            refused.map_err(rejection),
            Err(said(&Refused::Overdrawn { short: 10 }))
        );
        assert_eq!(withdrew, Ok(vec![any(Withdrew { cents: 20 })]));
        assert_eq!(rebuilt.state(), &holding(30, 2));
    }
}

mod snapshot {
    use super::*;

    /// With `snapshot = ..`, the state's snapshot is the user's conversion,
    /// under the message's type URL, and a stream resumed from it holds the
    /// same state.
    #[test]
    fn a_snapshot_form_round_trips_through_the_users_conversions() {
        let registry = registry();
        let mut wallet = opened(&registry, 0);
        run(&mut wallet, &registry, 2, any(Deposit { cents: 120 })).unwrap();

        let snapshot = wallet.snapshot(&registry).expect("the wallet snapshots");
        let resumed = Stream::<Wallet>::resume(
            wallet.cover().clone(),
            &snapshot,
            wallet.last_recorded_at(),
            &[],
            &registry,
        )
        .unwrap();

        let state = snapshot.state.clone().unwrap_or_default();
        assert_eq!(type_name(&state.type_url), "toy.wallet.v1.WalletSnapshot");
        assert_eq!(
            <Wallet as State>::descriptor().type_url,
            "/toy.wallet.v1.WalletSnapshot"
        );
        assert_eq!(
            wallet.state().to_snapshot(),
            wire::WalletSnapshot::from(wallet.state())
        );
        assert_eq!(resumed.state(), wallet.state());
    }

    /// Without `snapshot = ..`, the kernel takes none: every load replays.
    /// Its state's descriptor names the markup's `NoSnapshot`, in
    /// angzarr-embedded's own package.
    #[test]
    fn no_snapshot_form_means_no_snapshot() {
        let registry = registry();
        let mut lamp = Stream::<Lamp>::new(root(), None).unwrap();

        let on = run(&mut lamp, &registry, 1, any(SwitchOn {}));

        assert_eq!(on, Ok(vec![any(SwitchedOn {})]));
        assert!(lamp.snapshot(&registry).is_none());
        assert_eq!(
            <Lamp as State>::descriptor().type_url,
            "/angzarr_embedded.next.markup.NoSnapshot"
        );
    }
}

mod rejection {
    use super::*;

    /// A handler's `Err` is the domain's own rejection, as it returned it,
    /// and nothing is committed or applied. The rejection type is inferred
    /// from the handlers' `Result`s.
    #[test]
    fn a_rejection_is_typed_and_commits_nothing() {
        let registry = registry();
        let mut wallet = opened(&registry, 5);
        let before = wallet.state().clone();

        let again = run(&mut wallet, &registry, 2, any(Open { bonus: 5 }));
        let overdrawn = run(&mut wallet, &registry, 3, any(Withdraw { cents: 8 }));

        assert_eq!(again.map_err(rejection), Err(said(&Refused::AlreadyOpen)));
        assert_eq!(
            overdrawn.map_err(rejection),
            Err(said(&Refused::Overdrawn { short: 3 }))
        );
        assert_eq!(wallet.len(), 2, "nothing was committed");
        assert_eq!(wallet.state(), &before, "nothing was applied");
    }
}

mod failed {
    use super::*;

    fn meter_cover() -> Cover {
        Cover {
            domain: "meter".to_string(),
            root: Some(root()),
            edition: None,
        }
    }

    /// A handler's `Failed::Bug` is a bug, never a refusal: INTERNAL
    /// ROUTER_REASON_UNHANDLED_HANDLER_ERROR, `Recovery::Bug`, dead-lettered with the
    /// bug's code as its `cause` and its args as `cause.<arg>`, no English,
    /// and nothing written.
    #[test]
    fn a_handlers_bug_is_dead_lettered_and_writes_nothing() {
        let registry = registry();
        let store = Mem::new();
        let log = MemoryDeadLetters::new();

        let failure = handle_in::<Meter>(
            &store,
            &registry,
            &log,
            1,
            send("meter", 1, any(Gauge { reading: 0 }), None),
        )
        .err()
        .expect("the meter broke");

        assert!(
            matches!(
                &failure,
                Failure::Kernel(KernelError::Router(e)) if e.code == "ROUTER_REASON_UNHANDLED_HANDLER_ERROR"
            ),
            "a bug, not a refusal: {failure:?}"
        );
        assert_eq!(failure.recovery(), Recovery::Bug);
        broke_at(&log, 3);
        assert_eq!(pages(&store, &meter_cover()), None, "nothing written");
    }

    /// A handler's refusal under a status of the business set other than
    /// FAILED_PRECONDITION is still a business refusal, under that status:
    /// `Recovery::Business`, its own code and args, nothing dead-lettered
    /// and nothing written.
    #[test]
    fn a_refusal_goes_out_under_its_business_status() {
        let registry = registry();
        let store = Mem::new();
        let log = MemoryDeadLetters::new();

        let failure = handle_in::<Meter>(
            &store,
            &registry,
            &log,
            1,
            send("meter", 1, any(Gauge { reading: -4 }), None),
        )
        .err()
        .expect("refused");

        let Failure::Rejected(rejected) = &failure else {
            panic!("a business refusal, not {failure:?}");
        };
        assert_eq!(rejected.status(), BusinessStatus::InvalidArgument);
        assert_eq!(said(rejected), said(&Refused::NotPositive { cents: -4 }));
        assert_eq!(failure.recovery(), Recovery::Business);
        assert!(log.lines().is_empty(), "a refusal is not dead-lettered");
        assert_eq!(pages(&store, &meter_cover()), None, "nothing written");
    }

    /// The meter's one dead letter, a fatal bug of `seat`: its reason the
    /// router's key as the spec spells it, the bug's code its `cause` and
    /// its args `cause.<arg>`, and its one detail `processing_failed`,
    /// never retried, not transient (decision 14.48); no English.
    fn broke_at(log: &MemoryDeadLetters, seat: i64) {
        use crate::next::toys::{assert_letter, one_letter, processing_failed, wire};
        let lines = log.lines();
        assert_eq!(lines.len(), 1, "dead-lettered once: {lines:?}");
        let cause = format!(
            "meter: ROUTER_REASON_UNHANDLED_HANDLER_ERROR cause=toy-meter-broke cause.seat={seat}"
        );
        assert!(lines[0].contains(&cause), "{cause}: {}", lines[0]);
        assert!(
            !lines[0].contains("the meter at"),
            "no English: {}",
            lines[0]
        );
        let reason = wire(
            "ROUTER_REASON_UNHANDLED_HANDLER_ERROR",
            &[
                ("cause", "toy-meter-broke"),
                ("cause.seat", &seat.to_string()),
            ],
        );
        assert_letter(
            &one_letter(log.letters()),
            ::angzarr_embedded::next::v1::ComponentKind::Aggregate,
            &reason,
            processing_failed(&reason, 0, false),
        );
    }

    /// `book` (command 1) to the meter, failing.
    fn failing(store: &Mem, log: &MemoryDeadLetters, payload: Any) -> Failure<Rejected> {
        handle_in::<Meter>(store, &registry(), log, 1, send("meter", 1, payload, None))
            .err()
            .expect("failed")
    }

    /// A continuation step's bug fails the whole decision as a handler's
    /// does: ROUTER_REASON_UNHANDLED_HANDLER_ERROR, `Recovery::Bug`, dead-lettered with
    /// its cause, and nothing written, the handler's own event included.
    #[test]
    fn a_steps_bug_fails_the_whole_decision() {
        let store = Mem::new();
        let log = MemoryDeadLetters::new();

        let failure = failing(&store, &log, any(Tune { reading: 0 }));

        assert!(
            matches!(
                &failure,
                Failure::Kernel(KernelError::Router(e)) if e.code == "ROUTER_REASON_UNHANDLED_HANDLER_ERROR"
            ),
            "a bug, not a refusal: {failure:?}"
        );
        assert_eq!(failure.recovery(), Recovery::Bug);
        broke_at(&log, 1);
        assert_eq!(pages(&store, &meter_cover()), None, "nothing written");
    }

    /// A continuation step's refusal keeps its status: a business refusal
    /// of the whole decision, nothing dead-lettered or written.
    #[test]
    fn a_steps_refusal_keeps_its_status() {
        let store = Mem::new();
        let log = MemoryDeadLetters::new();

        let failure = failing(&store, &log, any(Tune { reading: -2 }));

        let Failure::Rejected(rejected) = &failure else {
            panic!("a business refusal, not {failure:?}");
        };
        assert_eq!(rejected.status(), BusinessStatus::InvalidArgument);
        assert_eq!(said(rejected), said(&Refused::NotPositive { cents: -2 }));
        assert_eq!(failure.recovery(), Recovery::Business);
        assert!(log.lines().is_empty(), "a refusal is not dead-lettered");
        assert_eq!(pages(&store, &meter_cover()), None, "nothing written");
    }

    /// A handler with `refuses(..)` fails with `Failed<Refuses<Self, Cmd>>`:
    /// its bug is a bug like any handler's.
    #[test]
    fn a_typed_refusal_handlers_bug_is_dead_lettered() {
        let store = Mem::new();
        let log = MemoryDeadLetters::new();

        let failure = failing(&store, &log, any(Seal { code: 0 }));

        assert!(
            matches!(
                &failure,
                Failure::Kernel(KernelError::Router(e)) if e.code == "ROUTER_REASON_UNHANDLED_HANDLER_ERROR"
            ),
            "a bug, not a refusal: {failure:?}"
        );
        assert_eq!(failure.recovery(), Recovery::Bug);
        broke_at(&log, 9);
        assert_eq!(pages(&store, &meter_cover()), None, "nothing written");
    }

    /// ... and its typed refusal goes out under its status, keyed
    /// ROUTER_REASON_COMMAND_REFUSED with its object's type, the object packed beside.
    #[test]
    fn a_typed_refusal_goes_out_under_its_status_with_its_object() {
        let store = Mem::new();
        let log = MemoryDeadLetters::new();

        let failure = failing(&store, &log, any(Seal { code: -1 }));

        let Failure::Rejected(rejected) = &failure else {
            panic!("a business refusal, not {failure:?}");
        };
        assert_eq!(rejected.status(), BusinessStatus::NotFound);
        assert_eq!(rejected.refusal::<MeterSealed>(), Some(MeterSealed {}));
        assert_eq!(
            said(rejected),
            (
                "angzarr-embedded-next-command-refused".to_string(),
                vec![(
                    "refusal_type".to_string(),
                    "toy.meter.v1.MeterSealed".to_string()
                )]
            )
        );
        assert_eq!(failure.recovery(), Recovery::Business);
        assert!(log.lines().is_empty(), "a refusal is not dead-lettered");
    }

    /// A failure reads as its refusal's code and args, whatever its
    /// status, or its bug's: never English.
    #[test]
    fn a_failure_reads_as_its_refusal_or_its_bug() {
        let refused: Failed<Refused> = Failed::from(Refused::Overdrawn { short: 2 });
        let invalid: Failed<Refused> =
            Failed::from(Refused::AlreadyOpen).with_status(BusinessStatus::InvalidArgument);
        let broke: Failed<Refused> = Failed::bug(&MeterBroke { seat: 4 });

        assert_eq!(refused.to_string(), "toy-wallet-overdrawn short=2");
        assert_eq!(invalid.to_string(), "toy-wallet-already-open");
        assert_eq!(broke.to_string(), "toy-meter-broke seat=4");
    }

    /// A retryable bug and an immediate return read as their report's code
    /// and args, whatever the status; `with_status` leaves them, and
    /// `widen` keeps them as they are, status and report.
    #[test]
    fn a_retryable_bug_and_an_immediate_return_read_as_their_report() {
        let offline: Failed<Refused> =
            Failed::retryable_bug(RetryableStatus::ResourceExhausted, &MeterBroke { seat: 4 });
        let moved: Failed<Refused> = Failed::immediate(&MeterMoved { seat: 2 });

        assert_eq!(offline.to_string(), "toy-meter-broke seat=4");
        assert_eq!(moved.to_string(), "toy-meter-moved seat=2");
        assert_eq!(
            offline.clone().with_status(BusinessStatus::NotFound),
            offline
        );
        assert_eq!(moved.clone().with_status(BusinessStatus::NotFound), moved);
        assert_eq!(
            offline.widen::<Refused>(),
            Failed::RetryableBug {
                status: RetryableStatus::ResourceExhausted,
                report: Report::of(&MeterBroke { seat: 4 }),
            }
        );
        assert_eq!(
            moved.widen::<Refused>(),
            Failed::Immediate(Report::of(&MeterMoved { seat: 2 }))
        );
    }

    /// `?` on the domain's own rejection inside a handler returning
    /// `Failed` refuses with it, FAILED_PRECONDITION; and a handler still
    /// returning its rejection as is refuses as before.
    #[test]
    fn the_domains_own_rejection_refuses_under_failed_precondition() {
        let registry = registry();
        let mut meter = Stream::<Meter>::new(root(), None).unwrap();

        let over = run(&mut meter, &registry, 1, any(Gauge { reading: 2000 }));
        let Err(Failure::Rejected(over)) = over else {
            panic!("refused by `?`, not {over:?}");
        };
        assert_eq!(over.status(), BusinessStatus::FailedPrecondition);
        assert_eq!(
            said(&over),
            said(&Refused::OverLimit {
                cents: 2000,
                limit: METER_LIMIT
            })
        );

        run(&mut meter, &registry, 2, any(Gauge { reading: 7 })).unwrap();
        let busy = run(&mut meter, &registry, 3, any(Calibrate {}));
        let Err(Failure::Rejected(busy)) = busy else {
            panic!("refused as before, not {busy:?}");
        };
        assert_eq!(busy.status(), BusinessStatus::FailedPrecondition);
        assert_eq!(said(&busy), said(&Refused::NotEmpty { cents: 7 }));
    }
}

mod generated {
    use super::*;

    fn deposited_through<H: WalletHandler>(handler: &mut H, event: Deposited) {
        handler.deposited(event);
    }

    fn withdraw_through<H: WalletHandler>(
        handler: &H,
        cmd: Withdraw,
        ctx: &Context,
    ) -> Result<Withdrew, Refused> {
        handler.withdraw(cmd, ctx)
    }

    /// `WalletHandler` names the marked set, and the wallet implements it
    /// by its own methods: through the trait, an applier and a handler do
    /// what the marked methods do.
    #[test]
    fn the_handler_trait_is_the_marked_methods() {
        let ctx = context(3);
        let mut through = Wallet {
            open: true,
            ..Wallet::default()
        };
        let mut direct = through.clone();

        deposited_through(
            &mut through,
            Deposited {
                cents: 10,
                at: at(2),
            },
        );
        direct.deposited(Deposited {
            cents: 10,
            at: at(2),
        });

        assert_eq!(through, direct);
        assert_eq!(
            withdraw_through(&through, Withdraw { cents: 4 }, &ctx),
            Ok(Withdrew { cents: 4 })
        );
        assert_eq!(
            withdraw_through(&through, Withdraw { cents: 11 }, &ctx),
            Err(Refused::Overdrawn { short: 1 })
        );
    }

    /// `HANDLES` (commands and facts) and `APPLIES` are the marked types'
    /// wire names, in marker order.
    #[test]
    fn handles_and_applies_are_the_marked_types_in_order() {
        let command = |name| TypeName {
            package: "toy.wallet.cmd.v1",
            name,
        };
        let event = |name| TypeName {
            package: "toy.wallet.v1",
            name,
        };

        assert_eq!(
            Wallet::HANDLES,
            [
                command("Open"),
                command("Deposit"),
                command("Withdraw"),
                command("Hold"),
                command("Sweep"),
                command("Close"),
                command("Note"),
                command("Spin"),
                TypeName {
                    package: "toy.wallet.fact.v1",
                    name: "Payout",
                },
                TypeName {
                    package: "toy.wallet.fact.v1",
                    name: "Gift",
                },
            ]
        );
        assert_eq!(
            Wallet::APPLIES,
            [
                event("Opened"),
                event("Deposited"),
                event("Rewarded"),
                event("Withdrew"),
                event("Held"),
                event("Closed"),
                event("Noted"),
                event("Spun"),
                event("PaidOut"),
                event("PayoutRefused"),
            ]
        );
    }
}

mod beside {
    use super::*;

    /// One registry holds generated domains and a hand-written one, and
    /// each decides and folds as its own impl says.
    #[test]
    fn a_hand_written_domain_works_beside_a_generated_one() {
        let registry = registry();
        let mut tally = Stream::<Tally>::new(root(), None).unwrap();
        let mut wallet = wallet();

        let counted = run(&mut tally, &registry, 1, pack(&Count {}));
        let opened = run(&mut wallet, &registry, 1, any(Open { bonus: 0 }));
        let counted_again = run(&mut tally, &registry, 2, pack(&Count {}));

        assert_eq!(counted, Ok(vec![pack(&Counted {})]));
        assert_eq!(
            opened,
            Ok(vec![
                any(Opened {}),
                any(Deposited {
                    cents: 0,
                    at: at(1)
                })
            ])
        );
        assert_eq!(counted_again, Ok(vec![pack(&Counted {})]));
        assert_eq!(tally.state().count, 2);
        assert!(wallet.state().open);
    }
}

// --------------------------------------------- the process manager and policies
//
// A PROCESS MANAGER IS A DOMAIN. The buy-in takes commands (which policies
// send it, and which it may refuse) and facts (a timeout; refusal facts),
// and records only its own events; it subscribes to nothing. Its root is
// the correlation, and only its declared starting inputs may start an
// instance. Two policies carry events across: the buy-in's into holds on the
// wallet (BuyInWallet), and the wallet's holds back into the buy-in
// (WalletBuyIn). When the wallet refuses a hold (with a typed
// InsufficientFunds, recording nothing), the policy answers one refusal fact
// for the buy-in: passed through as `CommandRefused`, handled by
// `#[on_refused(Hold)]`; or, where the policy has its own
// `#[on_refused(Hold)]` callback, the callback's domestic fact, `HoldLost`.
// What the policies answer is pinned here; running them, each step committed
// on its own, comes with the kernel's committed-step dispatch.
//
// Also pinned: translate is selected by the presence of the policy's
// callback, nothing else, and the callback returns a fact of its source;
// typed refusals are `Refuses<Self, Cmd>` in `markup::Rejection<R>`, the
// objects `Diagnostic`s; one marker, `#[on_refused]`, serves the source's
// handler and the policy's callback; angzarr-embedded does not check that a PM
// declares a starting input (angzarr-cli warns), so nothing here pins it;
// `Policy::on_refused` answers the fact's payload as `Any`; a
// `CommandRefused` from outside a policy is accepted when its trigger names
// the receiving stream; a policy names a PM target's root with the
// correlation.

/// A buy-in: a process across the wallet, keyed by its correlation.
#[derive(Clone, Default, PartialEq, Debug)]
pub struct BuyIn {
    started: bool,
    held: i64,
    failed: bool,
}

#[derive(Clone, PartialEq, Debug)]
pub enum BuyInRefused {
    AlreadyStarted,
}

impl fmt::Display for BuyInRefused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the buy-in has started already")
    }
}

impl Diagnostic for BuyInRefused {
    fn code(&self) -> &str {
        "toy-buyin-already-started"
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        vec![]
    }
}

#[process_manager(domain = "buyin")]
impl BuyIn {
    /// The starting input: a new correlation's buy-in begins here.
    #[handles_cmd(StartBuyIn, starts)]
    fn start(&self, cmd: StartBuyIn, _ctx: &Context) -> Result<BuyInStarted, BuyInRefused> {
        match self.started {
            true => Err(BuyInRefused::AlreadyStarted),
            false => Ok(BuyInStarted {
                wallet: cmd.wallet,
                cents: cmd.cents,
            }),
        }
    }

    /// Not a starting input: refused for a correlation with no buy-in.
    #[handles_cmd(RecordHeld)]
    fn record_held(&self, cmd: RecordHeld, _ctx: &Context) -> Result<BuyInHeld, BuyInRefused> {
        Ok(BuyInHeld { cents: cmd.cents })
    }

    /// A timeout, a fact, not a starting one: skipped for a correlation
    /// with no buy-in. Records the fact's origin, from the one context.
    #[handles_fact(BuyInExpired)]
    fn expired(&self, _fact: BuyInExpired, ctx: &Context) -> Record<Self> {
        Record::of(Expired {
            external_id: ctx
                .command
                .fact_origin
                .as_ref()
                .map(|origin| origin.external_id.clone())
                .unwrap_or_default(),
        })
    }

    /// The wallet refused the hold BuyInWallet sent: passed through, its
    /// typed refusal object unpacked here, and whether the buy-in had
    /// history read from the router's context. A wallet short by exactly
    /// 23 cannot happen: the handler's error, a bug.
    #[on_refused(Hold)]
    fn hold_refused(
        &self,
        refused: CommandRefused<Hold>,
        ctx: &Context,
    ) -> Result<Record<Self>, Report> {
        let short = refused
            .refusal::<InsufficientFunds>()
            .map(|object| object.short)
            .unwrap_or_default();
        if short == 23 {
            return Err(Report::new("toy-buyin-impossible-short").arg("short", short));
        }
        Ok(Record::of(BuyInFailed {
            short,
            had_history: ctx.command.had_prior_events,
        }))
    }

    /// The same refusal, translated by the policy into the buy-in's own
    /// fact: the buy-in never reads the wallet's refusal object.
    #[handles_fact(HoldLost)]
    fn hold_lost(&self, fact: HoldLost, _ctx: &Context) -> Record<Self> {
        Record::of(HoldWasLost { short: fact.short })
    }

    #[applies(BuyInStarted)]
    fn started(&mut self, _event: BuyInStarted) {
        self.started = true;
    }

    #[applies(BuyInHeld)]
    fn buy_in_held(&mut self, event: BuyInHeld) {
        self.held += event.cents;
    }

    #[applies(Expired)]
    fn expired_at(&mut self, _event: Expired) {}

    #[applies(BuyInFailed)]
    fn failed(&mut self, _event: BuyInFailed) {
        self.failed = true;
    }

    #[applies(HoldWasLost)]
    fn lost(&mut self, _event: HoldWasLost) {
        self.failed = true;
    }
}

/// The policy for (buy-in, wallet): a started buy-in holds its stake on the
/// wallet it names. Pass-through: a refused hold reaches the buy-in as
/// `CommandRefused`.
pub struct BuyInWallet;

#[policy(name = "buyin-wallet", source = BuyIn, target = Wallet)]
impl BuyInWallet {
    #[on_event(BuyInStarted)]
    fn started(
        &self,
        event: &BuyInStarted,
        _ctx: &PolicyContext,
    ) -> Result<Commands<Wallet>, Infallible> {
        Ok(Commands::none().to(uuid(&event.wallet), Hold { cents: event.cents }))
    }
}

/// As [`BuyInWallet`], with an `#[on_refused(Hold)]` callback, whose
/// presence selects translate for `Hold`: it reads the wallet's refusal
/// object and hands the buy-in its own fact.
pub struct TranslatingBuyInWallet;

#[policy(name = "buyin-wallet", source = BuyIn, target = Wallet)]
impl TranslatingBuyInWallet {
    #[on_event(BuyInStarted)]
    fn started(
        &self,
        event: &BuyInStarted,
        _ctx: &PolicyContext,
    ) -> Result<Commands<Wallet>, Infallible> {
        Ok(Commands::none().to(uuid(&event.wallet), Hold { cents: event.cents }))
    }

    /// A wallet short by exactly 23 cannot happen: the callback's error, a
    /// bug, naming the refused command it read from its context.
    #[on_refused(Hold)]
    fn hold_refused(
        &self,
        refusal: &Refusal<Hold>,
        ctx: &CommandContext,
    ) -> Result<HoldLost, Report> {
        let short = refusal
            .refusal::<InsufficientFunds>()
            .map(|object| object.short)
            .unwrap_or(refusal.command().cents);
        if short == 23 {
            let command = ctx.command_id.clone().unwrap_or_default().to_string();
            return Err(Report::new("toy-buyin-wallet-impossible-short")
                .arg("short", short)
                .arg("command", command));
        }
        Ok(HoldLost { short })
    }
}

/// The policy for (wallet, buy-in): a hold on the wallet is recorded on the
/// buy-in of its correlation. The buy-in's root IS that correlation.
pub struct WalletBuyIn;

#[policy(name = "wallet-buyin", source = Wallet, target = BuyIn)]
impl WalletBuyIn {
    #[on_event(Held)]
    fn held(&self, event: &Held, ctx: &PolicyContext) -> Result<Commands<BuyIn>, Infallible> {
        Ok(Commands::none().to(
            ctx.correlation_id().clone(),
            RecordHeld { cents: event.cents },
        ))
    }
}

/// `bytes` as a root.
fn uuid(bytes: &[u8]) -> Uuid {
    Uuid {
        value: bytes.to_vec().into(),
    }
}

/// Correlation `n`: the root of its buy-in.
fn correlation(n: u8) -> Uuid {
    Uuid::from_bytes([0xc0 | n; 16])
}

/// The buy-in's stream for correlation `c`.
fn buy_in_cover(c: &Uuid) -> Cover {
    Cover {
        domain: "buyin".to_string(),
        root: Some(c.clone()),
        edition: None,
    }
}

/// A book of `body` for the stream at `cover`, command id `[n; 16]`,
/// with `correlation` if given and `origin` for a fact.
fn book(
    cover: Cover,
    n: u8,
    body: command_page::Body,
    correlation: Option<Uuid>,
    origin: Option<FactOrigin>,
) -> CommandBook {
    let fact = origin.is_some();
    CommandBook {
        cover: Some(cover),
        page: Some(CommandPage {
            body: Some(body),
            command_id: (!fact).then(|| Uuid::from_bytes([n; 16])),
            correlation_id: correlation,
            fact_origin: origin,
            ..CommandPage::default()
        }),
        handling: None,
    }
}

/// Command `payload`, numbered `n`, to the stream at `cover`, in
/// `correlation`.
fn command_to(cover: Cover, n: u8, payload: Any, correlation: Option<Uuid>) -> CommandBook {
    book(
        cover,
        n,
        command_page::Body::Command(Command {
            payload: Some(payload),
        }),
        correlation,
        None,
    )
}

/// Fact `payload` from `external_id`, to the stream at `cover`.
fn fact_to(cover: Cover, payload: Any, external_id: &str) -> CommandBook {
    book(
        cover,
        0,
        command_page::Body::Fact(Fact {
            payload: Some(payload),
        }),
        None,
        Some(FactOrigin {
            external_id: external_id.to_string(),
            description: "toy".to_string(),
        }),
    )
}

/// Handle `book` (sampled at `n` seconds) through the kernel and `store`
/// on the async path, as an application does; `log` gets the dead
/// letters.
fn handle_in<D: Domain>(
    store: &Mem,
    registry: &Registry,
    log: &MemoryDeadLetters,
    n: u8,
    book: CommandBook,
) -> Result<Handled, Failure<Rejected>> {
    let sampled = Sampled {
        now: at(i64::from(n)),
        command_id: Uuid::from_bytes([0x70 | n; 16]),
    };
    ready(
        ::angzarr_embedded::next::Kernel::new(registry, log)
            .on_retryable(::angzarr_embedded::next::OnRetryable {
                max_retries: 0,
                ..::angzarr_embedded::next::OnRetryable::default()
            })
            .handle::<D, _, _>(store, book, &sampled, &mut ()),
    )
}

/// The stored pages of the stream at `cover`; `None` when it has none.
fn pages(store: &Mem, cover: &Cover) -> Option<Vec<EventPage>> {
    use ::angzarr_embedded::next::EventStore;
    let stream = ::angzarr_embedded::next::StreamRef::try_from(cover).unwrap();
    let slice = ready(store.read(stream, 0, None)).unwrap();
    (slice.next_sequence > 0).then_some(slice.pages)
}

/// The store the decorated domains run on: angzarr-ports' in memory.
type Mem = ::angzarr_embedded::next::angzarr_ports::memory::MemoryStore;

/// The type names of the events the buy-in of correlation `c` recorded;
/// `None` when it has no stream.
fn buy_in_events(store: &Mem, c: &Uuid) -> Option<Vec<String>> {
    pages(store, &buy_in_cover(c)).map(|pages| {
        pages
            .iter()
            .map(|p| type_name(&p.payload.as_ref().unwrap().type_url).to_string())
            .collect()
    })
}

/// The buy-in events, by name.
fn buy_in(names: &[&str]) -> Option<Vec<String>> {
    Some(names.iter().map(|n| format!("toy.buyin.v1.{n}")).collect())
}

/// The wallet and the buy-in, with no policy between them.
fn buy_ins() -> Registry {
    let mut registry = registry();
    registry.process_manager::<BuyIn>(BuyIn::default);
    registry
}

/// The wallet, the buy-in, both policies (`translating` picks the buy-in
/// policy's mode).
fn flows(translating: bool) -> Registry {
    let mut registry = buy_ins();
    registry.policy(WalletBuyIn);
    match translating {
        true => registry.policy(TranslatingBuyInWallet),
        false => registry.policy(BuyInWallet),
    };
    registry
}

/// `flows`, a store, and a wallet at [`root`] opened with `bonus`
/// (command 1, in a correlation of its own).
fn funded(translating: bool, bonus: i64) -> (Registry, Mem) {
    let registry = flows(translating);
    let store = Mem::new();
    let log = MemoryDeadLetters::new();
    handle_in::<Wallet>(
        &store,
        &registry,
        &log,
        1,
        send("wallet", 1, any(Open { bonus }), Some(correlation(15))),
    )
    .unwrap();
    (registry, store)
}

/// StartBuyIn for `cents` from the wallet at [`root`], as command `n` to
/// the buy-in of correlation `c`.
fn start(c: &Uuid, n: u8, cents: i64) -> CommandBook {
    command_to(
        buy_in_cover(c),
        n,
        any(StartBuyIn {
            wallet: root().value.to_vec(),
            cents,
        }),
        Some(c.clone()),
    )
}

mod process_manager {
    use super::*;

    /// A starting command for a correlation with no buy-in starts one, at
    /// the root that IS the correlation: no derivation.
    #[test]
    fn a_starting_command_starts_an_instance_at_its_correlation() {
        let registry = buy_ins();
        let store = Mem::new();
        let log = MemoryDeadLetters::new();
        let started = handle_in::<BuyIn>(&store, &registry, &log, 2, start(&correlation(1), 2, 0));
        assert!(
            started.is_ok(),
            "{:?}",
            started.err().map(|e| e.to_string())
        );
        assert_eq!(
            buy_in_events(&store, &correlation(1)).map(|names| names[0].clone()),
            Some("toy.buyin.v1.BuyInStarted".to_string())
        );
    }

    /// A command that is not a starting one, for a correlation with no
    /// instance, is refused (`COORDINATOR_REASON_PM_NO_INSTANCE`, a business refusal) and
    /// records nothing. Its code is pinned as a string.
    #[test]
    fn a_non_starting_command_for_no_instance_is_refused() {
        let registry = buy_ins();
        let store = Mem::new();
        let log = MemoryDeadLetters::new();
        let held = command_to(
            buy_in_cover(&correlation(2)),
            2,
            any(RecordHeld { cents: 5 }),
            Some(correlation(2)),
        );
        let Err(refused) = handle_in::<BuyIn>(&store, &registry, &log, 2, held) else {
            panic!("a command for no instance is refused");
        };
        assert_eq!(refused.code(), "angzarr-embedded-next-pm-no-instance");
        assert_eq!(refused.recovery(), Recovery::Business);
        assert_eq!(buy_in_events(&store, &correlation(2)), None);
    }

    /// A fact that is not a starting one, for a correlation with no
    /// instance, is skipped silently: applied, nothing recorded, nothing
    /// dead-lettered.
    #[test]
    fn a_non_starting_fact_for_no_instance_is_skipped() {
        let registry = buy_ins();
        let store = Mem::new();
        let log = MemoryDeadLetters::new();
        let expired = fact_to(
            buy_in_cover(&correlation(3)),
            any(BuyInExpired {}),
            "timeout-3",
        );
        let skipped = handle_in::<BuyIn>(&store, &registry, &log, 2, expired);
        assert!(
            matches!(skipped, Ok(Handled::Applied(_))),
            "{:?}",
            skipped.err().map(|e| e.to_string())
        );
        assert_eq!(buy_in_events(&store, &correlation(3)), None);
        assert!(log.lines().is_empty());
    }

    /// In memory, the kernel's start rule reads whether the stream holds
    /// an instance: a non-starting command is refused on an empty stream,
    /// and decided once a starting one made the instance.
    #[test]
    fn a_streams_instance_is_what_the_start_rule_reads() {
        let registry = buy_ins();
        let log = MemoryDeadLetters::new();
        let mut buy_in = Stream::<BuyIn>::new(correlation(11), None).unwrap();
        let sampled = Sampled {
            now: at(3),
            command_id: Uuid::from_bytes([0x73; 16]),
        };
        let held = |n| {
            command_to(
                buy_in_cover(&correlation(11)),
                n,
                any(RecordHeld { cents: 5 }),
                Some(correlation(11)),
            )
        };
        let refused = buy_in
            .execute(held(2), &registry, &sampled, &log, &mut [])
            .ready()
            .expect_err("no instance");
        assert_eq!(refused.code(), "angzarr-embedded-next-pm-no-instance");
        buy_in
            .execute(
                start(&correlation(11), 3, 0),
                &registry,
                &sampled,
                &log,
                &mut [],
            )
            .ready()
            .unwrap();
        assert!(buy_in
            .execute(held(4), &registry, &sampled, &log, &mut [])
            .ready()
            .is_ok());
        assert_eq!(buy_in.len(), 2);
    }

    /// A correlation left unset is given the root: the buy-in's events
    /// carry it.
    #[test]
    fn an_unset_correlation_is_given_the_root() {
        let registry = buy_ins();
        let store = Mem::new();
        let log = MemoryDeadLetters::new();
        let mut unset = start(&correlation(6), 2, 0);
        unset.page.as_mut().unwrap().correlation_id = None;
        handle_in::<BuyIn>(&store, &registry, &log, 2, unset).unwrap();
        let pages = pages(&store, &buy_in_cover(&correlation(6))).expect("the buy-in");
        assert_eq!(
            pages[0]
                .meta
                .as_ref()
                .and_then(|m| m.correlation_id.clone()),
            Some(correlation(6))
        );
    }

    /// The one handler context carries a fact's identity: its origin
    /// reaches the buy-in's fact handler.
    #[test]
    fn a_facts_origin_reaches_its_handler() {
        let registry = buy_ins();
        let store = Mem::new();
        let log = MemoryDeadLetters::new();
        handle_in::<BuyIn>(&store, &registry, &log, 2, start(&correlation(7), 2, 0)).unwrap();
        let expired = fact_to(
            buy_in_cover(&correlation(7)),
            any(BuyInExpired {}),
            "timeout-7",
        );
        let Ok(Handled::Applied(recorded)) =
            handle_in::<BuyIn>(&store, &registry, &log, 3, expired)
        else {
            panic!("the timeout is applied");
        };
        assert_eq!(
            recorded.books()[0]
                .pages
                .last()
                .and_then(|p| p.payload.clone()),
            Some(any(Expired {
                external_id: "timeout-7".to_string()
            }))
        );
    }

    /// A command that does not start one is decided once there is an
    /// instance.
    #[test]
    fn a_non_starting_command_is_decided_by_an_instance() {
        let registry = buy_ins();
        let store = Mem::new();
        let log = MemoryDeadLetters::new();
        handle_in::<BuyIn>(&store, &registry, &log, 2, start(&correlation(9), 2, 0)).unwrap();
        let held = command_to(
            buy_in_cover(&correlation(9)),
            3,
            any(RecordHeld { cents: 5 }),
            Some(correlation(9)),
        );
        handle_in::<BuyIn>(&store, &registry, &log, 3, held).unwrap();
        assert_eq!(
            buy_in_events(&store, &correlation(9)),
            buy_in(&["BuyInStarted", "BuyInHeld"])
        );
    }

    /// A command sent again with its id is a duplicate, as on any domain:
    /// nothing runs or is recorded twice.
    #[test]
    fn a_command_sent_again_is_a_duplicate() {
        let registry = buy_ins();
        let store = Mem::new();
        let log = MemoryDeadLetters::new();
        handle_in::<BuyIn>(&store, &registry, &log, 2, start(&correlation(8), 2, 0)).unwrap();
        let again = handle_in::<BuyIn>(&store, &registry, &log, 3, start(&correlation(8), 2, 0));
        assert!(matches!(again, Ok(Handled::Duplicate(_))));
        assert_eq!(
            buy_in_events(&store, &correlation(8)),
            buy_in(&["BuyInStarted"])
        );
    }

    /// `STARTS` names the starting inputs, `HANDLES` every command and
    /// fact, and `REFUSALS` the commands whose refusals it handles, each in
    /// marker order.
    #[test]
    fn its_constants_name_its_inputs() {
        let name = |name| TypeName {
            package: "toy.buyin.cmd.v1",
            name,
        };
        assert_eq!(<BuyIn as ProcessManager>::STARTS, [name("StartBuyIn")]);
        assert_eq!(
            BuyIn::HANDLES,
            [
                name("StartBuyIn"),
                name("RecordHeld"),
                name("BuyInExpired"),
                name("HoldLost"),
            ]
        );
        assert_eq!(
            BuyIn::REFUSALS,
            [TypeName {
                package: "toy.wallet.cmd.v1",
                name: "Hold",
            }]
        );
    }

    #[test]
    #[should_panic(expected = "buyin")]
    fn registering_a_process_manager_twice_panics() {
        let mut registry = registry();
        registry
            .process_manager::<BuyIn>(BuyIn::default)
            .process_manager::<BuyIn>(BuyIn::default);
    }
}

mod policy_registration {
    use super::*;

    #[test]
    #[should_panic(expected = "buyin-wallet")]
    fn registering_a_policy_twice_panics() {
        let mut registry = registry();
        registry.policy(BuyInWallet).policy(BuyInWallet);
    }
}

mod policy_events {
    use super::*;

    fn ctx(c: &Uuid) -> PolicyContext {
        PolicyContext::new(trigger(c), c.clone(), at(1))
    }

    /// A policy answers an event of its source with its `#[on_event]`
    /// handler's commands, each with the root of its stream.
    #[test]
    fn a_policy_answers_its_handlers_commands() {
        let started = any(BuyInStarted {
            wallet: root().value.to_vec(),
            cents: 30,
        });
        let commands = BuyInWallet
            .on_event(&started, &ctx(&correlation(1)))
            .unwrap()
            .expect("the policy handles BuyInStarted");
        assert_eq!(commands.orders(), [(root(), any(Hold { cents: 30 }))]);
    }

    /// A policy addressing a process manager names the instance by the
    /// event's correlation, its root.
    #[test]
    fn a_policy_names_a_process_managers_root_with_the_correlation() {
        let commands = WalletBuyIn
            .on_event(&any(Held { cents: 5 }), &ctx(&correlation(7)))
            .unwrap()
            .expect("the policy handles Held");
        assert_eq!(
            commands.orders(),
            [(correlation(7), any(RecordHeld { cents: 5 }))]
        );
    }

    /// A policy's context names the root of the stream its trigger was
    /// recorded on, the source's: what a policy addressing the source's
    /// own counterpart reads, with no unpacking of the trigger.
    #[test]
    fn a_policy_context_names_the_triggers_root() {
        let source = Uuid::from_bytes([0x5a; 16]);
        let trigger = EventRef {
            stream: Some(Cover {
                domain: "wallet".to_string(),
                root: Some(source.clone()),
                edition: None,
            }),
            sequence: 3,
        };
        let ctx = PolicyContext::new(trigger, correlation(1), at(1));
        assert_eq!(ctx.source_root(), &source);
    }

    /// A context whose trigger names no stream (one only a test builds)
    /// has the empty root.
    #[test]
    fn a_trigger_with_no_stream_has_the_empty_root() {
        let ctx = PolicyContext::new(EventRef::default(), correlation(1), at(1));
        assert_eq!(ctx.source_root(), &Uuid::default());
    }

    /// An event the policy has no handler for answers nothing.
    #[test]
    fn an_event_it_does_not_handle_answers_nothing() {
        assert!(BuyInWallet
            .on_event(&any(BuyInHeld { cents: 5 }), &ctx(&correlation(1)))
            .unwrap()
            .is_none());
    }
}

/// A policy whose `#[on_event]` handler fails: a buy-in started with no
/// stake cannot happen, so it is a bug, not a command to send.
pub struct FussyBuyInWallet;

#[policy(name = "fussy-buyin-wallet", source = BuyIn, target = Wallet)]
impl FussyBuyInWallet {
    #[on_event(BuyInStarted)]
    fn started(
        &self,
        event: &BuyInStarted,
        _ctx: &PolicyContext,
    ) -> Result<Commands<Wallet>, Report> {
        match event.cents > 0 {
            true => Ok(Commands::none().to(uuid(&event.wallet), Hold { cents: event.cents })),
            false => Err(Report::new("toy-buyin-no-stake").arg("cents", event.cents)),
        }
    }
}

mod policy_errors {
    use super::*;

    /// A marked-up policy's handler that fails answers its error: its
    /// code and its args, for the kernel to dead-letter; its `Display` is
    /// those, never English (decision 14.37).
    #[test]
    fn a_policy_handlers_error_is_its_answer() {
        let started = any(BuyInStarted {
            wallet: root().value.to_vec(),
            cents: 0,
        });
        let ctx = PolicyContext::new(trigger(&correlation(1)), correlation(1), at(1));
        let error = FussyBuyInWallet
            .on_event(&started, &ctx)
            .err()
            .expect("the handler failed");
        assert_eq!(
            said(&error),
            (
                "toy-buyin-no-stake".to_string(),
                vec![("cents".to_string(), "0".to_string())]
            )
        );
        assert_eq!(error.to_string(), "toy-buyin-no-stake cents=0");
    }

    /// An event of a type the policy marks that does not decode is its
    /// error, a bug: never skipped as an event it does not handle.
    #[test]
    fn a_marked_event_that_does_not_decode_is_the_policys_error() {
        let corrupt = Any {
            type_url: any(BuyInStarted {
                wallet: root().value.to_vec(),
                cents: 30,
            })
            .type_url,
            value: vec![0xff, 0xff, 0xff],
        };
        let ctx = PolicyContext::new(trigger(&correlation(1)), correlation(1), at(1));
        let error = BuyInWallet
            .on_event(&corrupt, &ctx)
            .err()
            .expect("an event it marks that does not decode fails it");
        assert_eq!(error.code(), "angzarr-embedded-next-payload-decode");
        assert!(
            error.args().iter().any(|(name, arg)| *name == "type_url"
                && matches!(arg, Arg::Str(url) if *url == corrupt.type_url)),
            "{:?}",
            error.args()
        );
    }

    /// The same handler, on an event it can answer, answers its commands.
    #[test]
    fn a_fallible_policy_handlers_commands_are_its_answer() {
        let started = any(BuyInStarted {
            wallet: root().value.to_vec(),
            cents: 30,
        });
        let ctx = PolicyContext::new(trigger(&correlation(1)), correlation(1), at(1));
        let commands = FussyBuyInWallet
            .on_event(&started, &ctx)
            .unwrap()
            .expect("the policy handles BuyInStarted");
        assert_eq!(commands.orders(), [(root(), any(Hold { cents: 30 }))]);
    }
}

/// Every handler reads angzarr-router's whole `CommandContext`, its
/// `Context`'s `command`: under AGGREGATE_HANDLES the conflict window and nonce it must
/// acknowledge, and under any resolution the stream's cover, the next
/// sequence, whether the stream has history, the expected sequence and the
/// resolution.
mod handler_context {
    use super::*;
    use ::angzarr_embedded::next::v1::Handling;

    fn pot() -> Stream<Pot> {
        Stream::new(root(), None).unwrap()
    }

    fn pot_cover() -> Cover {
        Cover {
            domain: "pot".to_string(),
            root: Some(root()),
            edition: None,
        }
    }

    /// Stake `n` of `cents`, issued against `expected`, asking for
    /// AGGREGATE_HANDLES; acknowledging its window or not.
    fn staking(n: u8, cents: i64, acknowledge: bool, expected: u64) -> CommandBook {
        let mut book = send("pot", n, any(Stake { cents, acknowledge }), None);
        book.handling = Some(Handling {
            expected_sequence: Some(expected),
            conflict_resolution: ConflictResolution::AggregateHandles as i32,
        });
        book
    }

    /// A stake at the head under AGGREGATE_HANDLES (the window empty, the
    /// nonce still delivered) and one behind it (a window of one) are each
    /// accepted once the handler acknowledges its window through
    /// `ctx.conflict`.
    #[test]
    fn an_aggregate_handles_handler_acknowledges_its_window_and_is_accepted() {
        let registry = registry();
        let mut pot = pot();
        assert_eq!(
            execute(&mut pot, &registry, 1, staking(1, 5, true, 0)),
            Ok(vec![any(Staked {
                cents: 5,
                window: 0
            })]),
            "the first stake, at the head: acknowledged, accepted"
        );
        assert_eq!(
            execute(&mut pot, &registry, 2, staking(2, 7, true, 0)),
            Ok(vec![any(Staked {
                cents: 7,
                window: 1
            })]),
            "issued before the first: it saw the one event it missed"
        );
    }

    /// A handler that never acknowledges its window has its output
    /// discarded: the router's COORDINATOR_REASON_CONFLICT_UNRESOLVED, the kernel's
    /// `Unresolved`, dead-lettered under that key with the command's
    /// expected sequence and the head as its `sequence_mismatch` (decision
    /// 14.48), and nothing recorded.
    #[test]
    fn an_aggregate_handles_handler_that_never_acknowledges_is_unresolved() {
        use crate::next::toys::{assert_letter, one_letter, sequence_mismatch, wire};
        let registry = registry();
        let mut pot = pot();
        execute(&mut pot, &registry, 1, staking(1, 5, true, 0)).unwrap();
        let log = MemoryDeadLetters::new();
        let sampled = Sampled {
            now: at(2),
            command_id: Uuid::from_bytes([0x08; 16]),
        };
        let Err(failure) = pot
            .execute(staking(2, 5, false, 0), &registry, &sampled, &log, &mut [])
            .ready()
        else {
            panic!("unresolved");
        };
        assert_eq!(
            failure,
            Failure::Kernel(KernelError::Unresolved {
                stream: pot_cover(),
                expected: 0,
                actual: 1,
            })
        );
        let letter = one_letter(log.letters());
        assert_letter(
            &letter,
            ::angzarr_embedded::next::v1::ComponentKind::Aggregate,
            &wire("COORDINATOR_REASON_CONFLICT_UNRESOLVED", &[]),
            sequence_mismatch(0, 1),
        );
        assert_eq!(pot.len(), 1, "nothing recorded past the first stake");
    }

    /// A handler reads its stream's domain and root from the router's
    /// cover.
    #[test]
    fn a_handler_reads_its_stream_root_from_the_context() {
        let registry = registry();
        let mut pot = pot();
        assert_eq!(
            run(&mut pot, &registry, 1, any(Mark {})),
            Ok(vec![any(Marked {
                root: root().value.to_vec(),
                domain: "pot".to_string(),
            })])
        );
    }

    /// A STRICT handler reads the router's context too: the next sequence,
    /// whether the stream has history, the sequence the command was issued
    /// against, the resolution, and no conflict.
    #[test]
    fn a_strict_handler_receives_the_context_too() {
        let registry = registry();
        let mut pot = pot();
        assert_eq!(
            run(&mut pot, &registry, 1, any(Peek {})),
            Ok(vec![any(Peeked {
                next_sequence: 0,
                had_prior_events: false,
                expected_sequence: 0,
                resolution: ConflictResolution::Strict as i32,
                conflict: false,
            })]),
            "a new stream"
        );
        assert_eq!(
            run(&mut pot, &registry, 2, any(Peek {})),
            Ok(vec![any(Peeked {
                next_sequence: 1,
                had_prior_events: true,
                expected_sequence: 1,
                resolution: ConflictResolution::Strict as i32,
                conflict: false,
            })]),
            "a stream with one event"
        );
    }
}

mod typed_refusals {
    use super::*;
    use ::angzarr_embedded::next::v1::Diagnostic as WireDiagnostic;

    /// As [`BuyInWallet`], hand-written, and answering no refusal: its
    /// refused hold fails its step and is dead-lettered.
    struct QuietBuyInWallet;

    impl Policy for QuietBuyInWallet {
        const NAME: &'static str = "quiet-buyin-wallet";
        type Source = BuyIn;
        type Target = Wallet;

        fn on_event(
            &self,
            event: &Any,
            _ctx: &PolicyContext,
        ) -> Result<Option<Commands<Wallet>>, Report> {
            let Ok(Some(started)) = markup::decode_message::<BuyInStarted>(event) else {
                return Ok(None);
            };
            Ok(Some(Commands::none().to(
                uuid(&started.wallet),
                Hold {
                    cents: started.cents,
                },
            )))
        }
    }

    /// A typed refusal of a policy's command that no OnRefused answers is
    /// dead-lettered under ROUTER_REASON_COMMAND_REFUSED with its
    /// `refusal_type`, and its one detail is `command_refused`: the type and
    /// the refusal object itself, kept whole (decision 14.48, C-0733).
    #[test]
    fn a_typed_refusal_dead_letters_its_object_as_command_refused() {
        use crate::next::toys::{one_letter, wire};
        use ::angzarr_embedded::next::v1::angzarr_dead_letter::RejectionDetails;
        use ::angzarr_embedded::next::v1::CommandRefusedDetails;
        let mut registry = buy_ins();
        registry.policy(QuietBuyInWallet);
        let store = Mem::new();
        let log = MemoryDeadLetters::new();
        handle_in::<Wallet>(
            &store,
            &registry,
            &log,
            1,
            send("wallet", 1, any(Open { bonus: 0 }), Some(correlation(15))),
        )
        .unwrap();
        let _ = handle_in::<BuyIn>(&store, &registry, &log, 2, start(&correlation(3), 2, 50));
        let letter = one_letter(log.letters());
        let reason = wire(
            "ROUTER_REASON_COMMAND_REFUSED",
            &[("refusal_type", "toy.wallet.v1.InsufficientFunds")],
        );
        crate::next::toys::assert_letter(
            &letter,
            ::angzarr_embedded::next::v1::ComponentKind::Policy,
            &reason,
            RejectionDetails::CommandRefused(CommandRefusedDetails {
                refusal_type: "toy.wallet.v1.InsufficientFunds".to_string(),
                refusal: Some(any(InsufficientFunds { short: 50 })),
            }),
        );
        assert_eq!(letter.source_component, "quiet-buyin-wallet");
    }

    /// A typed refusal is keyed, as on the wire, ROUTER_REASON_COMMAND_REFUSED with its
    /// object's full type name as `refusal_type`; a message-only refusal by
    /// its own code and args. Each displays its code and args.
    #[test]
    fn a_typed_refusal_is_keyed_command_refused_with_its_type() {
        let typed = Rejection::<Refused>::Typed(markup::TypedRefusal::of(&WalletClosed {}));
        assert_eq!(typed.code(), "angzarr-embedded-next-command-refused");
        assert!(
            matches!(&typed.args()[..], [("refusal_type", Arg::Str(t))] if t == "toy.wallet.v1.WalletClosed"),
            "{:?}",
            typed.args()
        );
        assert_eq!(
            typed.to_string(),
            "angzarr-embedded-next-command-refused refusal_type=toy.wallet.v1.WalletClosed"
        );
        let message = Rejection::<Refused>::Message(Refused::Overdrawn { short: 20 });
        assert_eq!(message.to_string(), "toy-wallet-overdrawn short=20");
    }

    /// A translating callback reads an untyped refusal's code and args as
    /// its diagnostic; a typed one has none, and its object instead.
    #[test]
    fn a_refusals_diagnostic_is_an_untyped_refusals_code_and_args() {
        let overdrawn = WireDiagnostic {
            code: "toy-wallet-overdrawn".to_string(),
            args: [("short".to_string(), "20".to_string())].into(),
        };
        let untyped = Refusal::new(
            EventBook::default(),
            Withdraw { cents: 30 },
            RefusalResponse {
                reason: Some(refusal_response::Reason::Diagnostic(overdrawn.clone())),
            },
        );
        assert_eq!(untyped.diagnostic(), Some(&overdrawn));
        assert_eq!(untyped.refusal::<InsufficientFunds>(), None);
        let typed = Refusal::new(
            EventBook::default(),
            Withdraw { cents: 30 },
            insufficient(20),
        );
        assert_eq!(typed.diagnostic(), None);
        assert_eq!(
            typed.refusal::<InsufficientFunds>(),
            Some(InsufficientFunds { short: 20 })
        );
    }

    /// A refusal's response is the oneof (decision 14.37): a typed
    /// refusal's object, or an untyped refusal's diagnostic, its code and
    /// args; never English. As the markup builds it and as the kernel hands
    /// it to a refused policy alike.
    #[test]
    fn a_refusals_response_is_its_object_or_its_diagnostic() {
        let overdrawn = || {
            Some(refusal_response::Reason::Diagnostic(WireDiagnostic {
                code: "toy-wallet-overdrawn".to_string(),
                args: [("short".to_string(), "20".to_string())].into(),
            }))
        };
        let message = Rejection::<Refused>::Message(Refused::Overdrawn { short: 20 });
        assert_eq!(message.response().reason, overdrawn());

        let registry = registry();
        let mut wallet = opened(&registry, 10);
        let Err(Failure::Rejected(untyped)) =
            run(&mut wallet, &registry, 2, any(Withdraw { cents: 30 }))
        else {
            panic!("refused");
        };
        assert_eq!(untyped.response().reason, overdrawn());
        let mut closed = super::wallet();
        let Err(Failure::Rejected(typed)) = run(&mut closed, &registry, 1, any(Hold { cents: 1 }))
        else {
            panic!("refused");
        };
        assert_eq!(
            typed.response().reason,
            Some(refusal_response::Reason::Refusal(any(WalletClosed {})))
        );
    }

    /// A handler with `refuses` refuses with one of its typed objects, as
    /// the caller receives it; the wallet records nothing.
    #[test]
    fn a_typed_refusal_carries_its_object_and_records_nothing() {
        let registry = registry();
        let mut wallet = opened(&registry, 10);
        let refused = run(&mut wallet, &registry, 2, any(Hold { cents: 30 }));
        match refused {
            Err(Failure::Rejected(typed)) => {
                assert_eq!(
                    typed.refusal::<InsufficientFunds>(),
                    Some(InsufficientFunds { short: 20 })
                );
                assert_eq!(typed.refusal::<WalletClosed>(), None);
            }
            other => panic!("a typed refusal, not {other:?}"),
        }
        assert_eq!(wallet.len(), 2, "nothing was recorded");
    }

    /// As a refused policy receives it: the object packed; keyed
    /// ROUTER_REASON_COMMAND_REFUSED with the object's type as `refusal_type`, as on the
    /// wire, and its `Display` is that code and args, never English
    /// (decision 14.37).
    #[test]
    fn a_typed_refusals_response_packs_its_object_and_message() {
        let registry = registry();
        let mut wallet = wallet();
        let refused = run(&mut wallet, &registry, 1, any(Hold { cents: 1 }));
        let Err(Failure::Rejected(rejection)) = refused else {
            panic!("refused");
        };
        assert_eq!(
            (rejection.refusal_object().cloned(), rejection.to_string()),
            (
                Some(any(WalletClosed {})),
                "angzarr-embedded-next-command-refused refusal_type=toy.wallet.v1.WalletClosed"
                    .to_string()
            )
        );
    }

    /// A handler without `refuses` refuses with the domain's own rejection,
    /// a message only: no object.
    #[test]
    fn a_message_only_refusal_has_no_object() {
        let registry = registry();
        let mut wallet = opened(&registry, 10);
        let Err(Failure::Rejected(rejection)) =
            run(&mut wallet, &registry, 2, any(Withdraw { cents: 30 }))
        else {
            panic!("refused");
        };
        assert_eq!(said(&rejection), said(&Refused::Overdrawn { short: 20 }));
        assert!(
            matches!(&rejection.args()[..], [("short", Arg::Str(short))] if short == "20"),
            "its args, as the router carried them: {:?}",
            rejection.args()
        );
        assert_eq!(
            (rejection.refusal_object().cloned(), rejection.to_string()),
            (None, "toy-wallet-overdrawn short=20".to_string())
        );
    }

    /// `REFUSES` names a command's typed refusal objects, in order.
    #[test]
    fn refuses_names_the_commands_refusal_objects() {
        let object = |name| TypeName {
            package: "toy.wallet.v1",
            name,
        };
        assert_eq!(
            <Wallet as Refusing<Hold>>::REFUSES,
            [object("InsufficientFunds"), object("WalletClosed")]
        );
    }
}

/// The buy-in's first event at correlation `c` (sequence 0), as a policy's
/// trigger.
fn trigger(c: &Uuid) -> EventRef {
    EventRef {
        stream: Some(buy_in_cover(c)),
        sequence: 0,
    }
}

/// A refusal of a hold for `cents`, sent by the buy-in policy for the
/// buy-in at correlation `c`, with `response`.
fn hold_refusal(c: &Uuid, cents: i64, response: RefusalResponse) -> markup::v1::Refusal {
    markup::v1::Refusal {
        event: Some(EventBook {
            cover: Some(buy_in_cover(c)),
            pages: vec![EventPage {
                sequence: 0,
                payload: Some(any(BuyInStarted {
                    wallet: root().value.to_vec(),
                    cents,
                })),
                ..EventPage::default()
            }],
            ..EventBook::default()
        }),
        command: Some(send("wallet", 9, any(Hold { cents }), Some(c.clone()))),
        response: Some(response),
    }
}

fn insufficient(short: i64) -> RefusalResponse {
    RefusalResponse {
        reason: Some(refusal_response::Reason::Refusal(any(InsufficientFunds {
            short,
        }))),
    }
}

mod policy_refusals {
    use super::*;

    /// Pass-through (the default): the generated `on_refused` answers
    /// angzarr's `CommandRefused { command, refusal, trigger }`, the
    /// refused command as sent, its typed object still packed, the trigger
    /// the triggering event; the message stays behind. The answer is the
    /// fact's payload, as `Any`.
    #[test]
    fn pass_through_builds_command_refused_for_the_source() {
        let refusal = hold_refusal(&correlation(1), 30, insufficient(20));
        let fact = BuyInWallet
            .on_refused(&refusal, &CommandContext::default())
            .unwrap();
        assert_eq!(
            fact,
            pack(&markup::v1::CommandRefused {
                command: refusal.command.clone(),
                refusal: Some(any(InsufficientFunds { short: 20 })),
                trigger: Some(trigger(&correlation(1))),
            })
        );
    }

    /// Translate (selected by the callback's presence): it reads the typed
    /// object and answers the source's own fact.
    #[test]
    fn translate_answers_the_callbacks_domestic_fact() {
        let refusal = hold_refusal(&correlation(1), 30, insufficient(20));
        assert_eq!(
            TranslatingBuyInWallet
                .on_refused(&refusal, &CommandContext::default())
                .unwrap(),
            any(HoldLost { short: 20 })
        );
    }

    /// A translating callback's error is its answer, a bug: its code and
    /// args, the refused command named from the context it was handed.
    #[test]
    fn a_translating_callbacks_error_is_its_answer() {
        let refusal = hold_refusal(&correlation(1), 30, insufficient(23));
        let ctx = CommandContext {
            command_id: Some(Uuid::from_bytes([0x09; 16])),
            ..CommandContext::default()
        };
        let error = TranslatingBuyInWallet
            .on_refused(&refusal, &ctx)
            .expect_err("the callback failed");
        assert_eq!(error.code(), "toy-buyin-wallet-impossible-short");
        let args = error.args();
        assert!(
            matches!(args.as_slice(), [("short", Arg::Int(23)), ("command", Arg::Str(id))]
                if *id == Uuid::from_bytes([0x09; 16]).to_string()),
            "{args:?}"
        );
    }

    /// A refused command of a type the policy translates that does not
    /// decode is its error, a bug: never passed through as one it does not
    /// translate.
    #[test]
    fn a_translated_refused_command_that_does_not_decode_is_the_policys_error() {
        let mut refusal = hold_refusal(&correlation(1), 30, insufficient(20));
        let corrupt = Any {
            type_url: any(Hold { cents: 30 }).type_url,
            value: vec![0xff, 0xff, 0xff],
        };
        refusal.command = Some(send("wallet", 9, corrupt.clone(), Some(correlation(1))));
        let error = TranslatingBuyInWallet
            .on_refused(&refusal, &CommandContext::default())
            .expect_err("a command it translates that does not decode fails it");
        assert_eq!(error.code(), "angzarr-embedded-next-payload-decode");
        let args = error.args();
        assert!(
            args.iter().any(|(name, arg)| *name == "type_url"
                && matches!(arg, Arg::Str(url) if *url == corrupt.type_url)),
            "{args:?}"
        );
    }

    /// A policy's constants: what events it handles, and what it translates, the
    /// latter derived from its `#[on_refused]` callbacks.
    #[test]
    fn its_constants_name_its_events_and_translations() {
        let started = TypeName {
            package: "toy.buyin.v1",
            name: "BuyInStarted",
        };
        let hold = TypeName {
            package: "toy.wallet.cmd.v1",
            name: "Hold",
        };
        assert_eq!(<BuyInWallet as Policy>::EVENTS, [started]);
        assert_eq!(
            <WalletBuyIn as Policy>::EVENTS,
            [TypeName {
                package: "toy.wallet.v1",
                name: "Held",
            }]
        );
        assert_eq!(<BuyInWallet as Policy>::TRANSLATES, []);
        assert_eq!(<TranslatingBuyInWallet as Policy>::TRANSLATES, [hold]);
    }

    /// The source's `#[on_refused(Hold)]` is chosen by the refused
    /// command's type, and reads the typed object: a `CommandRefused` of a
    /// hold, delivered to the buy-in its trigger names, records the
    /// failure. Delivered from outside a policy, it is accepted because
    /// its trigger names the receiving stream.
    #[test]
    fn on_refused_dispatches_by_the_refused_commands_type() {
        let registry = buy_ins();
        let store = Mem::new();
        let log = MemoryDeadLetters::new();
        handle_in::<BuyIn>(&store, &registry, &log, 2, start(&correlation(2), 2, 30)).unwrap();
        let refused = BuyInWallet
            .on_refused(
                &hold_refusal(&correlation(2), 30, insufficient(20)),
                &CommandContext::default(),
            )
            .unwrap();
        handle_in::<BuyIn>(
            &store,
            &registry,
            &log,
            3,
            fact_to(buy_in_cover(&correlation(2)), refused, "refusal-2"),
        )
        .unwrap();
        assert_eq!(
            buy_in_events(&store, &correlation(2)),
            buy_in(&["BuyInStarted", "BuyInFailed"])
        );
        let pages = pages(&store, &buy_in_cover(&correlation(2))).expect("the buy-in");
        assert_eq!(
            pages[1].payload,
            Some(any(BuyInFailed {
                short: 20,
                had_history: true
            })),
            "the handler read the typed refusal object, and the router's context"
        );
    }

    /// A refusal of a command the source has no `#[on_refused]` for is
    /// an unknown type, named by the refused command's type: what the
    /// dispatch dead-letters as a refusal with no path.
    #[test]
    fn a_refusal_with_no_handler_is_the_refused_commands_unknown_type() {
        let registry = buy_ins();
        let store = Mem::new();
        let log = MemoryDeadLetters::new();
        handle_in::<BuyIn>(&store, &registry, &log, 2, start(&correlation(8), 2, 30)).unwrap();
        let mut refusal = hold_refusal(&correlation(8), 30, insufficient(20));
        refusal.command = Some(send(
            "wallet",
            9,
            any(Withdraw { cents: 30 }),
            Some(correlation(8)),
        ));
        let refused = BuyInWallet
            .on_refused(&refusal, &CommandContext::default())
            .unwrap();
        let unhandled = handle_in::<BuyIn>(
            &store,
            &registry,
            &log,
            3,
            fact_to(buy_in_cover(&correlation(8)), refused, "refusal-8"),
        );
        match unhandled {
            Err(Failure::Kernel(KernelError::Router(e))) => {
                assert_eq!(e.code, "ROUTER_REASON_NO_REFUSAL_HANDLER");
                assert_eq!(
                    e.extras.get("command_type").map(String::as_str),
                    Some("/toy.wallet.cmd.v1.Withdraw")
                );
            }
            Err(other) => panic!("the router's ROUTER_REASON_NO_REFUSAL_HANDLER, not {other}"),
            Ok(_) => panic!("the router's ROUTER_REASON_NO_REFUSAL_HANDLER, not handled"),
        }
    }

    /// The buy-in's fact set reads angzarr's `CommandRefused`, beside its
    /// own facts: it has an `#[on_refused]` handler.
    #[test]
    fn a_fact_set_with_refusal_handlers_reads_command_refused() {
        let mut facts = <<BuyIn as Domain>::Fact as Payload>::type_urls();
        facts.sort();
        assert_eq!(
            facts,
            [
                "/io.angzarr.v1.CommandRefused",
                "/toy.buyin.cmd.v1.BuyInExpired",
                "/toy.buyin.cmd.v1.HoldLost",
            ]
        );
    }

    /// A refusal fact is accepted only by the exact stream its trigger
    /// names: delivered anywhere else, it is malformed.
    #[test]
    fn a_refusal_fact_for_another_stream_is_malformed() {
        let registry = buy_ins();
        let store = Mem::new();
        let log = MemoryDeadLetters::new();
        handle_in::<BuyIn>(&store, &registry, &log, 2, start(&correlation(3), 2, 30)).unwrap();
        let elsewhere = BuyInWallet
            .on_refused(
                &hold_refusal(&correlation(4), 30, insufficient(20)),
                &CommandContext::default(),
            )
            .unwrap();
        let Err(refused) = handle_in::<BuyIn>(
            &store,
            &registry,
            &log,
            3,
            fact_to(buy_in_cover(&correlation(3)), elsewhere, "refusal-3"),
        ) else {
            panic!("a refusal fact for another stream is malformed");
        };
        assert_eq!(
            refused.code(),
            "angzarr-embedded-next-malformed-refusal-misaddressed"
        );
        assert_eq!(
            buy_in_events(&store, &correlation(3)),
            buy_in(&["BuyInStarted"])
        );
    }
}

/// Handle `book` (sampled at `n` seconds): its own step, then every policy
/// step it sets off, each committed on its own, waiting for the chain to
/// settle, as an application does; `log` gets the dead letters.
fn settle_in<D: Domain>(
    store: &Mem,
    registry: &Registry,
    log: &MemoryDeadLetters,
    n: u8,
    book: CommandBook,
) -> Result<::angzarr_embedded::next::Recorded, Failure<Rejected>> {
    let sampled = Sampled {
        now: at(i64::from(n)),
        command_id: Uuid::from_bytes([0x70 | n; 16]),
    };
    let handled = ready(
        ::angzarr_embedded::next::Kernel::new(registry, log)
            .on_retryable(::angzarr_embedded::next::OnRetryable {
                max_retries: 0,
                ..::angzarr_embedded::next::OnRetryable::default()
            })
            .handle::<D, _, _>(store, book, &sampled, &mut ()),
    )?;
    match handled {
        Handled::Applied(recorded) => Ok(recorded),
        Handled::Duplicate(_) | Handled::Parked => {
            Ok(::angzarr_embedded::next::Recorded::default())
        }
    }
}

/// The covers of the steps `settled` committed, in commit order, each
/// with the type names of its events.
fn steps(settled: &::angzarr_embedded::next::Recorded) -> Vec<(String, Vec<String>)> {
    settled
        .steps()
        .iter()
        .map(|book| {
            (
                book.cover.as_ref().unwrap().domain.clone(),
                book.pages
                    .iter()
                    .map(|p| type_name(&p.payload.as_ref().unwrap().type_url).to_string())
                    .collect(),
            )
        })
        .collect()
}

mod refusal_flow {
    use super::*;

    /// A policy's command the target accepts flows on, each step committed
    /// on its own, in order: the buy-in's start, then the hold the buy-in's
    /// policy sends the wallet, then the record the wallet's policy sends back
    /// to the buy-in of its correlation (the policy names the buy-in's root
    /// with the correlation).
    #[test]
    fn an_accepted_command_flows_through_both_policies_step_by_step() {
        let (registry, store) = funded(false, 50);
        let log = MemoryDeadLetters::new();
        let settled =
            settle_in::<BuyIn>(&store, &registry, &log, 2, start(&correlation(1), 2, 30)).unwrap();
        assert_eq!(
            steps(&settled),
            [
                ("buyin", "toy.buyin.v1.BuyInStarted"),
                ("wallet", "toy.wallet.v1.Held"),
                ("buyin", "toy.buyin.v1.BuyInHeld"),
            ]
            .map(|(domain, event)| (domain.to_string(), vec![event.to_string()]))
        );
        assert_eq!(
            buy_in_events(&store, &correlation(1)),
            buy_in(&["BuyInStarted", "BuyInHeld"])
        );
    }

    /// Pass-through: the buy-in's start is committed first; the wallet
    /// refuses the hold and records nothing; the policy's `CommandRefused`
    /// reaches the buy-in's `#[on_refused(Hold)]` as a step of its own,
    /// which records the failure.
    #[test]
    fn a_refused_command_reaches_the_sources_on_refused_handler() {
        let (registry, store) = funded(false, 10);
        let log = MemoryDeadLetters::new();
        let settled = settle_in::<BuyIn>(&store, &registry, &log, 2, start(&correlation(2), 2, 30));
        let Ok(settled) = settled else {
            panic!("{:?}", settled.err().map(|e| e.to_string()));
        };
        assert_eq!(
            steps(&settled),
            [
                ("buyin", "toy.buyin.v1.BuyInStarted"),
                ("buyin", "toy.buyin.v1.BuyInFailed"),
            ]
            .map(|(domain, event)| (domain.to_string(), vec![event.to_string()]))
        );
        assert_eq!(
            pages(&store, wallet().cover()).map(|p| p.len()),
            Some(2),
            "the wallet recorded nothing of the hold"
        );
    }

    /// The source's `#[on_refused]` handler's error is the policy's failure,
    /// a bug: the step fails as `PolicyFailed`, a defect, is dead-lettered
    /// with the error's code and args (no English), records nothing, and
    /// the policy's position moves past the trigger, so a resume does not
    /// run it again.
    #[test]
    fn a_refusal_handlers_error_dead_letters_its_step_as_the_policys_failure() {
        let (registry, store) = funded(false, 7);
        let log = MemoryDeadLetters::new();
        let failure = settle_in::<BuyIn>(&store, &registry, &log, 2, start(&correlation(9), 2, 30))
            .err()
            .expect("the refusal handler's error fails the call");
        let Failure::Downstream { error, .. } = &failure else {
            panic!("a downstream failure: {failure:?}");
        };
        let StepError::PolicyFailed(report) = error else {
            panic!("the policy's failure: {error:?}");
        };
        assert_eq!(report.code(), "toy-buyin-impossible-short");
        assert_eq!(failure.recovery(), Recovery::Bug);
        assert_eq!(
            buy_in_events(&store, &correlation(9)),
            buy_in(&["BuyInStarted"]),
            "the refusal step recorded nothing"
        );
        let lines = log.lines();
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert!(
            lines[0].contains(
                "buyin: ROUTER_REASON_UNHANDLED_HANDLER_ERROR cause=toy-buyin-impossible-short cause.short=23"
            ),
            "a handler's bug, its code and args the cause: {}",
            lines[0]
        );
        assert!(
            !lines[0].contains("cannot happen"),
            "no English: {}",
            lines[0]
        );

        let sampled = Sampled {
            now: at(9),
            command_id: Uuid::from_bytes([0x79; 16]),
        };
        let resumed = ready(
            ::angzarr_embedded::next::Kernel::new(&registry, &log).resume(
                &store,
                &sampled,
                &mut (),
            ),
        )
        .expect("nothing left owed");
        assert!(
            resumed.steps().is_empty(),
            "the position moved past the trigger"
        );
        assert_eq!(log.lines().len(), 1, "nothing dead-lettered again");
    }

    /// Under `OnPolicyBug::Stay`, the source's `#[on_refused]` handler's
    /// error is still the policy's failure, dead-lettered, but the
    /// policy's position stays at the trigger, so a resume runs the step
    /// again and dead-letters it again.
    #[test]
    fn under_stay_a_refusal_handlers_error_holds_the_policys_position() {
        use ::angzarr_embedded::next::{Kernel, OnPolicyBug};
        let (registry, store) = funded(false, 7);
        let log = MemoryDeadLetters::new();
        let kernel = Kernel::new(&registry, &log).on_policy_bug(OnPolicyBug::Stay);
        let sampled = Sampled {
            now: at(2),
            command_id: Uuid::from_bytes([0x72; 16]),
        };
        let failure = ready(kernel.handle::<BuyIn, _, _>(
            &store,
            start(&correlation(9), 2, 30),
            &sampled,
            &mut (),
        ))
        .err()
        .expect("the refusal handler's error fails the call");
        assert!(
            matches!(
                &failure,
                Failure::Downstream {
                    error: StepError::PolicyFailed(_),
                    ..
                }
            ),
            "{failure:?}"
        );
        assert_eq!(log.lines().len(), 1, "{:?}", log.lines());
        let resumed = ready(kernel.resume(&store, &sampled, &mut ()))
            .err()
            .expect("the position stayed, so the step runs again");
        assert!(
            matches!(
                &resumed,
                Failure::Downstream {
                    error: StepError::PolicyFailed(_),
                    ..
                }
            ),
            "{resumed:?}"
        );
        assert_eq!(
            log.lines().len(),
            2,
            "dead-lettered again: {:?}",
            log.lines()
        );
    }

    /// The refusal fact's id is the refused command's id, and its origin's
    /// external id that id's canonical text.
    #[test]
    fn the_refusal_facts_id_is_the_refused_commands() {
        let (registry, store) = funded(false, 10);
        let log = MemoryDeadLetters::new();
        settle_in::<BuyIn>(&store, &registry, &log, 2, start(&correlation(3), 2, 30)).unwrap();
        let pages = pages(&store, &buy_in_cover(&correlation(3))).expect("the buy-in");
        let hold_id = ::angzarr_embedded::next::ids::policy_command_id(
            &trigger(&correlation(3)),
            "buyin-wallet",
            0,
        )
        .unwrap();
        let meta = pages[1].meta.clone().expect("meta");
        assert_eq!(meta.causation, Some(Causation::CommandId(hold_id.clone())));
        assert_eq!(
            meta.fact.map(|origin| origin.external_id),
            Some(hold_id.to_string())
        );
    }

    /// Translate: the policy's callback hands the buy-in its own fact,
    /// which its `#[handles_fact(HoldLost)]` records, as its own step.
    #[test]
    fn translate_delivers_the_callbacks_domestic_fact() {
        let (registry, store) = funded(true, 10);
        let log = MemoryDeadLetters::new();
        let settled =
            settle_in::<BuyIn>(&store, &registry, &log, 2, start(&correlation(4), 2, 30)).unwrap();
        assert_eq!(
            steps(&settled).last(),
            Some(&(
                "buyin".to_string(),
                vec!["toy.buyin.v1.HoldWasLost".to_string()]
            ))
        );
        assert_eq!(
            buy_in_events(&store, &correlation(4)),
            buy_in(&["BuyInStarted", "HoldWasLost"])
        );
    }

    /// A refusal with no path: a hold on the wallet outside any buy-in is
    /// committed, the wallet's policy sends `RecordHeld` to a correlation
    /// with no buy-in, which refuses it, and the wallet handles no refusal
    /// of `RecordHeld`. The hold stays committed, the fact is
    /// dead-lettered, and the call is Ok.
    #[test]
    fn a_refusal_with_no_handler_is_dead_lettered() {
        let (registry, store) = funded(false, 50);
        let log = MemoryDeadLetters::new();
        let settled = settle_in::<Wallet>(
            &store,
            &registry,
            &log,
            2,
            send("wallet", 2, any(Hold { cents: 5 }), Some(correlation(5))),
        );
        let Ok(settled) = settled else {
            panic!("{:?}", settled.err().map(|e| e.to_string()));
        };
        assert_eq!(
            steps(&settled),
            [("wallet".to_string(), vec!["toy.wallet.v1.Held".to_string()])]
        );
        let lines = log.lines();
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert!(lines[0].contains(" stream=wallet/"), "{lines:?}");
    }

    /// The dead letter of a refusal with no path carries the code of what
    /// was missing (the router's `ROUTER_REASON_NO_REFUSAL_HANDLER`, as angzarr-embedded's
    /// code), never rendered English.
    #[test]
    fn a_refusal_with_no_handlers_dead_letter_carries_its_code() {
        let (registry, store) = funded(false, 50);
        let log = MemoryDeadLetters::new();
        let _ = settle_in::<Wallet>(
            &store,
            &registry,
            &log,
            2,
            send("wallet", 2, any(Hold { cents: 5 }), Some(correlation(5))),
        );
        let lines = log.lines();
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert!(
            lines[0].contains("wallet: ROUTER_REASON_NO_REFUSAL_HANDLER command_type="),
            "the router's key as the spec spells it: {lines:?}"
        );
        assert!(!lines[0].contains("nothing handles"), "{lines:?}");
    }

    /// The dead letter of a malformed policy book carries the error's code
    /// and its args, never its rendered English.
    #[test]
    fn a_malformed_policy_books_dead_letter_carries_its_code_and_args() {
        let mut registry = registry();
        registry.policy(WalletBuyIn);
        let (_, store) = funded(false, 50);
        let log = MemoryDeadLetters::new();
        let _ = settle_in::<Wallet>(
            &store,
            &registry,
            &log,
            2,
            send("wallet", 2, any(Hold { cents: 5 }), Some(correlation(6))),
        );
        let lines = log.lines();
        assert_eq!(lines.len(), 1, "{lines:?}");
        // A policy's command for a domain the kernel cannot route is the
        // spec's key (decision 14.49), naming the domain.
        assert!(
            lines[0].contains("COORDINATOR_REASON_POLICY_COMMAND_UNKNOWN_DOMAIN"),
            "{lines:?}"
        );
        assert!(lines[0].contains("domain=buyin"), "{lines:?}");
        assert!(!lines[0].contains("is registered"), "{lines:?}");
    }

    /// A step that fails mid-way keeps the commits before it: no rollback.
    /// The wallet's policy sends to the buy-in, which this registry lacks:
    /// the call is an error, the hold stays committed, and the policy's
    /// malformed command is dead-lettered.
    #[test]
    fn a_failing_step_keeps_the_commits_before_it() {
        let mut registry = registry();
        registry.policy(WalletBuyIn);
        let (_, store) = funded(false, 50);
        let log = MemoryDeadLetters::new();
        let settled = settle_in::<Wallet>(
            &store,
            &registry,
            &log,
            2,
            send("wallet", 2, any(Hold { cents: 5 }), Some(correlation(6))),
        );
        assert!(settled.is_err(), "the policy's step failed");
        assert_eq!(
            pages(&store, wallet().cover()).and_then(|p| p.last().and_then(|p| p.payload.clone())),
            Some(any(Held { cents: 5 })),
            "the hold stays committed"
        );
        assert_eq!(log.lines().len(), 1, "{:?}", log.lines());
    }
}

// ------------------------------------------------- through the router
//
// The kernel dispatches business through angzarr-router: handlers, typed
// refusals and the refusal path, deduplication, a process manager's start
// rule, a policy's commands kept in its trigger's edition, and their root
// and correlation (decision 14.36, as amended). What is pinned here is
// what the router owes the kernel's callers: a policy names the root of
// every command, for any target, and keeps its trigger's correlation; a
// process manager admits an input whose correlation is not its root, so
// one instance takes several correlations. And that the kernel runs the
// router itself: a hand-built `PolicyDispatch`, the router's own table,
// runs in the chain, and refusals only the router makes reach the caller
// as `KernelError::Router`, its `CodedError` unchanged.

/// The policy for (buy-in, wallet) that forgets the hold's root: a policy
/// bug.
pub struct RootlessHold;

#[policy(name = "rootless-hold", source = BuyIn, target = Wallet)]
impl RootlessHold {
    #[on_event(BuyInStarted)]
    fn started(
        &self,
        event: &BuyInStarted,
        _ctx: &PolicyContext,
    ) -> Result<Commands<Wallet>, Infallible> {
        Ok(Commands::none().to(Uuid::default(), Hold { cents: event.cents }))
    }
}

/// The policy for (wallet, buy-in) that forgets the buy-in's root: a policy
/// bug, for a process manager as for an aggregate.
pub struct RootlessRecord;

#[policy(name = "rootless-record", source = Wallet, target = BuyIn)]
impl RootlessRecord {
    #[on_event(Held)]
    fn held(&self, event: &Held, _ctx: &PolicyContext) -> Result<Commands<BuyIn>, Infallible> {
        Ok(Commands::none().to(Uuid::default(), RecordHeld { cents: event.cents }))
    }
}

/// The root of the one buy-in [`SharedBuyIn`] records every hold on.
fn shared_buy_in() -> Uuid {
    Uuid::from_bytes([0x5a; 16])
}

/// The policy for (wallet, buy-in) that records every hold, whatever its
/// correlation, on ONE buy-in: the root it names, not the correlation.
pub struct SharedBuyIn;

#[policy(name = "shared-buyin", source = Wallet, target = BuyIn)]
impl SharedBuyIn {
    #[on_event(Held)]
    fn held(&self, event: &Held, _ctx: &PolicyContext) -> Result<Commands<BuyIn>, Infallible> {
        Ok(Commands::none().to(shared_buy_in(), RecordHeld { cents: event.cents }))
    }
}

mod router {
    use super::*;
    use ::angzarr_embedded::next::angzarr_router::error::HandlerError;
    use ::angzarr_embedded::next::angzarr_router::pb;
    use ::angzarr_embedded::next::angzarr_router::policy::PolicyDispatch;
    use ::angzarr_embedded::next::StepError;

    /// The router's reason (its `CodedError.code`) for `failure`, at the
    /// origin or down the chain; `None` for a failure the router did not
    /// make.
    fn router_reason<R>(failure: &Failure<R>) -> Option<&str> {
        match failure {
            Failure::Kernel(KernelError::Router(error))
            | Failure::Downstream {
                error: StepError::Kernel(KernelError::Router(error)),
                ..
            } => Some(error.code.as_str()),
            _ => None,
        }
    }

    /// The wallet, the buy-in and `policy`, with the wallet at [`root`]
    /// opened with 50 (command 1, in a correlation of its own).
    fn opened_with<P: Policy>(policy: P) -> (Registry, Mem) {
        let mut registry = buy_ins();
        registry.policy(policy);
        let store = Mem::new();
        let log = MemoryDeadLetters::new();
        handle_in::<Wallet>(
            &store,
            &registry,
            &log,
            1,
            send("wallet", 1, any(Open { bonus: 50 }), Some(correlation(15))),
        )
        .unwrap();
        (registry, store)
    }

    /// The wallet's stream, as the router's wire types name it.
    fn wallet_cover(domain: &str) -> pb::Cover {
        pb::Cover {
            domain: domain.to_string(),
            root: Some(pb::Uuid {
                value: root().value.to_vec().into(),
            }),
            edition: None,
        }
    }

    /// A hand-built policy for (buy-in, wallet), the router's own table: a
    /// started buy-in holds its stake on the wallet. `astray` edits each
    /// command before it is answered.
    fn hold_dispatch(astray: fn(&mut pb::CommandBook)) -> PolicyDispatch {
        PolicyDispatch::new("hand-built-hold", "buyin", "wallet").on_event(
            "toy.buyin.v1.BuyInStarted",
            move |event, _source| -> Result<Vec<pb::CommandBook>, HandlerError> {
                let started =
                    <wire::BuyInStarted as prost::Message>::decode(event.value.as_slice())
                        .map_err(|e| HandlerError::Other(e.to_string()))?;
                let mut hold = pb::CommandBook {
                    cover: Some(wallet_cover("wallet")),
                    page: Some(pb::CommandPage {
                        body: Some(pb::command_page::Body::Command(pb::Command {
                            payload: Some(any(Hold {
                                cents: started.cents,
                            })),
                        })),
                        ..Default::default()
                    }),
                    handling: None,
                };
                astray(&mut hold);
                Ok(vec![hold])
            },
        )
    }

    /// The wallet, the buy-in and `dispatch`, with the wallet opened with
    /// 50.
    fn opened_with_dispatch(dispatch: PolicyDispatch) -> (Registry, Mem) {
        let mut registry = buy_ins();
        registry.policy_dispatch(dispatch);
        let store = Mem::new();
        let log = MemoryDeadLetters::new();
        handle_in::<Wallet>(
            &store,
            &registry,
            &log,
            1,
            send("wallet", 1, any(Open { bonus: 50 }), Some(correlation(15))),
        )
        .unwrap();
        (registry, store)
    }

    /// A policy's command to an aggregate with no root is the policy's bug,
    /// refused by the router (`COORDINATOR_REASON_POLICY_COMMAND_NO_ROOT`), dead-lettered and
    /// never delivered; the trigger's step stays committed.
    #[test]
    fn a_policy_command_to_an_aggregate_with_no_root_is_refused_no_root() {
        let (registry, store) = opened_with(RootlessHold);
        let log = MemoryDeadLetters::new();
        let Err(failed) =
            settle_in::<BuyIn>(&store, &registry, &log, 2, start(&correlation(1), 2, 30))
        else {
            panic!("a rootless policy command fails its step");
        };
        assert_eq!(
            router_reason(&failed),
            Some("COORDINATOR_REASON_POLICY_COMMAND_NO_ROOT"),
            "{failed}"
        );
        assert_eq!(failed.recovery(), Recovery::Bug);
        assert_eq!(
            buy_in_events(&store, &correlation(1)),
            buy_in(&["BuyInStarted"]),
            "the trigger's step stays committed"
        );
        assert_eq!(
            pages(&store, wallet().cover()).map(|p| p.len()),
            Some(2),
            "the wallet recorded nothing"
        );
        assert_eq!(log.lines().len(), 1, "{:?}", log.lines());
    }

    /// The same for a process manager's command: there is no default root,
    /// the trigger's correlation included.
    #[test]
    fn a_policy_command_to_a_process_manager_with_no_root_is_refused_no_root() {
        let (registry, store) = opened_with(RootlessRecord);
        let log = MemoryDeadLetters::new();
        let Err(failed) = settle_in::<Wallet>(
            &store,
            &registry,
            &log,
            2,
            send("wallet", 2, any(Hold { cents: 5 }), Some(correlation(2))),
        ) else {
            panic!("a rootless policy command fails its step");
        };
        assert_eq!(
            router_reason(&failed),
            Some("COORDINATOR_REASON_POLICY_COMMAND_NO_ROOT"),
            "{failed}"
        );
        assert_eq!(buy_in_events(&store, &correlation(2)), None);
        assert_eq!(log.lines().len(), 1, "{:?}", log.lines());
    }

    /// A policy's command carrying a correlation other than its trigger's is
    /// the policy's bug, refused by the router
    /// (`COORDINATOR_REASON_POLICY_COMMAND_OTHER_CORRELATION`), and never delivered.
    #[test]
    fn a_policy_command_in_another_correlation_is_refused_other_correlation() {
        let (registry, store) = opened_with_dispatch(hold_dispatch(|hold| {
            hold.page.as_mut().unwrap().correlation_id = Some(pb::Uuid {
                value: vec![0xee; 16].into(),
            });
        }));
        let log = MemoryDeadLetters::new();
        let Err(failed) =
            settle_in::<BuyIn>(&store, &registry, &log, 2, start(&correlation(3), 2, 30))
        else {
            panic!("a policy command in another correlation fails its step");
        };
        assert_eq!(
            router_reason(&failed),
            Some("COORDINATOR_REASON_POLICY_COMMAND_OTHER_CORRELATION"),
            "{failed}"
        );
        assert_eq!(
            pages(&store, wallet().cover()).map(|p| p.len()),
            Some(2),
            "the wallet recorded nothing"
        );
    }

    /// A process manager's input whose correlation is not its root is
    /// delivered to the root its cover names, keeping its correlation.
    #[test]
    fn a_process_manager_input_in_another_correlation_is_admitted() {
        let registry = buy_ins();
        let store = Mem::new();
        let log = MemoryDeadLetters::new();
        let mut elsewhere = start(&correlation(4), 2, 0);
        elsewhere.page.as_mut().unwrap().correlation_id = Some(correlation(5));
        let admitted = handle_in::<BuyIn>(&store, &registry, &log, 2, elsewhere);
        assert!(
            admitted.is_ok(),
            "{:?}",
            admitted.err().map(|e| e.to_string())
        );
        let pages = pages(&store, &buy_in_cover(&correlation(4))).expect("the buy-in");
        assert_eq!(
            pages[0]
                .meta
                .as_ref()
                .and_then(|m| m.correlation_id.clone()),
            Some(correlation(5)),
            "its events carry its correlation"
        );
    }

    /// One process-manager instance takes the inputs of several
    /// correlations: each hold, in its own correlation, is recorded on the
    /// one buy-in its policy names, and each event carries its input's
    /// correlation.
    #[test]
    fn several_correlations_reach_one_process_manager_instance() {
        let (registry, store) = opened_with(SharedBuyIn);
        let log = MemoryDeadLetters::new();
        handle_in::<BuyIn>(&store, &registry, &log, 2, start(&shared_buy_in(), 2, 0)).unwrap();
        for (n, c) in [(3, correlation(1)), (4, correlation(2))] {
            let settled = settle_in::<Wallet>(
                &store,
                &registry,
                &log,
                n,
                send("wallet", n, any(Hold { cents: 5 }), Some(c)),
            );
            assert!(
                settled.is_ok(),
                "{:?}",
                settled.err().map(|e| e.to_string())
            );
        }
        let pages = pages(&store, &buy_in_cover(&shared_buy_in())).expect("the buy-in");
        let correlations: Vec<_> = pages
            .iter()
            .map(|p| p.meta.as_ref().and_then(|m| m.correlation_id.clone()))
            .collect();
        assert_eq!(
            correlations,
            [
                Some(shared_buy_in()),
                Some(correlation(1)),
                Some(correlation(2))
            ]
        );
        assert_eq!(
            buy_in_events(&store, &shared_buy_in()),
            buy_in(&["BuyInStarted", "BuyInHeld", "BuyInHeld"])
        );
    }

    /// The kernel runs angzarr-router's own `PolicyDispatch`, built by hand:
    /// its command is delivered as a step of its own.
    #[test]
    fn the_kernel_runs_a_hand_built_policy_dispatch() {
        let (registry, store) = opened_with_dispatch(hold_dispatch(|_| {}));
        let log = MemoryDeadLetters::new();
        let settled = settle_in::<BuyIn>(&store, &registry, &log, 2, start(&correlation(6), 2, 30));
        let Ok(settled) = settled else {
            panic!("{:?}", settled.err().map(|e| e.to_string()));
        };
        assert_eq!(
            steps(&settled),
            [
                ("buyin", "toy.buyin.v1.BuyInStarted"),
                ("wallet", "toy.wallet.v1.Held"),
            ]
            .map(|(domain, event)| (domain.to_string(), vec![event.to_string()]))
        );
    }

    /// A refusal only the router makes reaches the caller unchanged: a
    /// command outside the policy's one destination
    /// (`COORDINATOR_REASON_POLICY_COMMAND_OUTSIDE_DESTINATION`, naming both domains).
    #[test]
    fn a_refusal_only_the_router_makes_reaches_the_caller() {
        let (registry, store) = opened_with_dispatch(hold_dispatch(|hold| {
            hold.cover = Some(wallet_cover("lamp"));
        }));
        let log = MemoryDeadLetters::new();
        let Err(failed) =
            settle_in::<BuyIn>(&store, &registry, &log, 2, start(&correlation(7), 2, 30))
        else {
            panic!("a command outside the destination fails its step");
        };
        assert_eq!(
            router_reason(&failed),
            Some("COORDINATOR_REASON_POLICY_COMMAND_OUTSIDE_DESTINATION"),
            "{failed}"
        );
        let Failure::Downstream {
            error: StepError::Kernel(KernelError::Router(error)),
            ..
        } = &failed
        else {
            unreachable!("the router's reason is a downstream router error");
        };
        assert_eq!(
            error.extras.get("expected_domain").map(String::as_str),
            Some("wallet")
        );
        assert_eq!(
            error.extras.get("actual_domain").map(String::as_str),
            Some("lamp")
        );
        let routed = KernelError::Router(error.clone());
        let args = routed.args();
        assert!(
            args.iter()
                .any(|(name, arg)| *name == "actual_domain"
                    && matches!(arg, Arg::Str(d) if d == "lamp")),
            "the router's metadata, as args: {args:?}"
        );
    }

    /// A handler the kernel knows only as angzarr-router's `CommandHandler`
    /// (a remote one, say), written by hand, registered by its descriptor
    /// ([`remote_descriptor`]: its state and its rules version); it answers every command with one
    /// `Noted` event.
    struct Remote;

    impl ::angzarr_embedded::next::angzarr_router::router::CommandHandler for Remote {
        fn name(&self) -> &str {
            "remote"
        }

        fn command_types(&self) -> Vec<String> {
            vec!["toy.wallet.cmd.v1.Note".to_string()]
        }

        async fn dispatch(
            &self,
            req: &pb::ContextualCommand,
        ) -> Result<
            pb::CommandHandlerResponse,
            ::angzarr_embedded::next::angzarr_router::error::CodedError,
        > {
            let next = req.events.as_ref().map_or(0, |b| b.next_sequence);
            Ok(pb::CommandHandlerResponse {
                conflict: None,
                events: Some(pb::EventBook {
                    pages: vec![pb::EventPage {
                        payload: Some(any(Noted {
                            root: vec![next as u8],
                            command_id: Vec::new(),
                            correlation_id: Vec::new(),
                        })),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                rules_version: "remote-1".to_string(),
            })
        }
    }

    /// [`Remote`]'s descriptor, as angzarr-build generates it (decision
    /// 14.42): what every remote handler here registers by.
    fn remote_descriptor() -> pb::ComponentDescriptor {
        pb::ComponentDescriptor {
            name: "remote".to_string(),
            kind: pb::ComponentKind::Aggregate as i32,
            rules_version: "remote-1".to_string(),
            state: Some(StateDescriptor {
                type_url: "/toy.remote.v1.State".to_string(),
                options: Some(StateOptions {
                    conflict_resolutions: vec![
                        ConflictResolution::Strict as i32,
                        ConflictResolution::AggregateHandles as i32,
                    ],
                }),
            }),
            ..Default::default()
        }
    }

    /// A hand-written `CommandHandler`, boxed as `dyn`, registers as a
    /// domain: the kernel dispatches its commands through it, stamps and
    /// writes what it answers, and deduplicates a retry.
    #[test]
    fn a_boxed_command_handler_registers_as_a_domain() {
        use ::angzarr_embedded::next::angzarr_router::router::DynCommandHandler;
        let mut registry = Registry::new();
        let remote: Box<dyn DynCommandHandler> = Box::new(Remote);
        registry
            .command_handler("remote", remote, remote_descriptor())
            .unwrap();
        let store = Mem::new();
        let log = MemoryDeadLetters::new();
        let kernel = ::angzarr_embedded::next::Kernel::new(&registry, &log);
        let note = || send("remote", 4, any(Note {}), Some(correlation(4)));
        let sampled = Sampled {
            now: at(9),
            command_id: Uuid::from_bytes([0x74; 16]),
        };
        let handled = ready(kernel.handle_any(&store, note(), &sampled, &mut ())).unwrap();
        assert!(matches!(handled, Handled::Applied(_)));
        let remote_cover = Cover {
            domain: "remote".to_string(),
            root: Some(root()),
            edition: None,
        };
        let written = pages(&store, &remote_cover).expect("the remote's stream");
        assert_eq!(written.len(), 1);
        assert_eq!(
            written[0].payload,
            Some(any(Noted {
                root: vec![0],
                command_id: Vec::new(),
                correlation_id: Vec::new(),
            }))
        );
        let meta = written[0].meta.clone().expect("stamped");
        assert_eq!(meta.rules_version, "remote-1");
        assert_eq!(
            meta.causation,
            Some(Causation::CommandId(Uuid::from_bytes([4; 16])))
        );
        assert_eq!(meta.correlation_id, Some(correlation(4)));
        let again = ready(kernel.handle_any(&store, note(), &sampled, &mut ())).unwrap();
        assert!(
            matches!(again, Handled::Duplicate(_)),
            "a retry is a duplicate"
        );
        assert_eq!(
            registry.command_handler("remote", Remote, remote_descriptor()),
            Err(::angzarr_embedded::next::Misconfigured::AlreadyRegistered {
                domain: "remote".to_string()
            })
        );
    }

    /// A [`Remote`] that keeps the `ContextualCommand.recorded_at` of each
    /// input it is handed.
    struct Clocked(std::sync::Arc<std::sync::Mutex<Vec<Option<Timestamp>>>>);

    impl ::angzarr_embedded::next::angzarr_router::router::CommandHandler for Clocked {
        fn name(&self) -> &str {
            "remote"
        }

        fn command_types(&self) -> Vec<String> {
            Remote.command_types()
        }

        async fn dispatch(
            &self,
            req: &pb::ContextualCommand,
        ) -> Result<
            pb::CommandHandlerResponse,
            ::angzarr_embedded::next::angzarr_router::error::CodedError,
        > {
            self.0.lock().unwrap().push(req.recorded_at);
            Remote.dispatch(req).await
        }
    }

    /// The kernel stamps the input's acceptance time on the
    /// `ContextualCommand` it hands the handler (decision 14.37,
    /// `ContextualCommand.recorded_at`), and its events carry that same
    /// instant as their `EventMeta.recorded_at`, held to `max(the stream's
    /// last, now)`: a later input sampled earlier is stamped at the last.
    #[test]
    fn the_kernel_stamps_the_inputs_acceptance_time_on_the_contextual_command() {
        let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut registry = Registry::new();
        registry
            .command_handler(
                "remote",
                Clocked(std::sync::Arc::clone(&seen)),
                remote_descriptor(),
            )
            .unwrap();
        let store = Mem::new();
        let log = MemoryDeadLetters::new();
        let kernel = ::angzarr_embedded::next::Kernel::new(&registry, &log);
        let sampled = |now| Sampled {
            now,
            command_id: Uuid::from_bytes([0x74; 16]),
        };
        let note = |id| send("remote", id, any(Note {}), Some(correlation(4)));
        ready(kernel.handle_any(&store, note(4), &sampled(at(9)), &mut ())).unwrap();
        ready(kernel.handle_any(&store, note(5), &sampled(at(5)), &mut ())).unwrap();
        assert_eq!(
            *seen.lock().unwrap(),
            [Some(at(9)), Some(at(9))],
            "stamped on the input, held to the stream's last"
        );
        let remote_cover = Cover {
            domain: "remote".to_string(),
            root: Some(root()),
            edition: None,
        };
        let written = pages(&store, &remote_cover).expect("the remote's stream");
        let stamped: Vec<_> = written
            .iter()
            .map(|page| page.meta.as_ref().and_then(|m| m.recorded_at))
            .collect();
        assert_eq!(stamped, [Some(at(9)), Some(at(9))], "the events' instant");
    }

    /// A remote handler that cannot be reached, until `up`: it answers
    /// angzarr-embedded-host's `COORDINATOR_REASON_HANDLER_UNAVAILABLE`
    /// (UNAVAILABLE, its cause in its metadata); reached, it answers as [`Remote`].
    struct Flaky {
        up: std::sync::Arc<std::sync::atomic::AtomicBool>,
    }

    impl ::angzarr_embedded::next::angzarr_router::router::CommandHandler for Flaky {
        fn name(&self) -> &str {
            "remote"
        }

        fn command_types(&self) -> Vec<String> {
            Remote.command_types()
        }

        async fn dispatch(
            &self,
            req: &pb::ContextualCommand,
        ) -> Result<
            pb::CommandHandlerResponse,
            ::angzarr_embedded::next::angzarr_router::error::CodedError,
        > {
            use ::angzarr_embedded::next::angzarr_router::error::{CodedError, GrpcCode};
            if self.up.load(std::sync::atomic::Ordering::SeqCst) {
                return Remote.dispatch(req).await;
            }
            Err(CodedError {
                code: "COORDINATOR_REASON_HANDLER_UNAVAILABLE".to_string(),
                extras: [
                    ("domain".to_string(), "remote".to_string()),
                    ("cause".to_string(), "not-running".to_string()),
                ]
                .into(),
                grpc: GrpcCode::Unavailable,
                refusal: None,
            })
        }
    }

    /// An unreachable remote handler is a retryable failure, never a
    /// rejection. With retrying off, at the origin the caller retries; as a
    /// policy's step the position holds, nothing is dead-lettered, and a
    /// resume runs the step again once the handler is reachable.
    #[test]
    fn an_unreachable_remote_handler_is_retried_by_position() {
        use ::angzarr_embedded::next::angzarr_router::router::DynCommandHandler;
        use ::angzarr_embedded::next::StepError;
        let up = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut registry = registry();
        let flaky: Box<dyn DynCommandHandler> = Box::new(Flaky { up: up.clone() });
        registry
            .command_handler("remote", flaky, remote_descriptor())
            .unwrap();
        registry.policy_dispatch(
            PolicyDispatch::new("wallet-remote", "wallet", "remote").on_event(
                "toy.wallet.v1.Held",
                |_, source| {
                    Ok(vec![pb::CommandBook {
                        cover: Some(pb::Cover {
                            domain: "remote".to_string(),
                            root: source.and_then(|c| c.root.clone()),
                            edition: None,
                        }),
                        page: Some(pb::CommandPage {
                            body: Some(pb::command_page::Body::Command(pb::Command {
                                payload: Some(any(Note {})),
                            })),
                            ..Default::default()
                        }),
                        handling: None,
                    }])
                },
            ),
        );
        let store = Mem::new();
        let log = MemoryDeadLetters::new();
        let kernel = ::angzarr_embedded::next::Kernel::new(&registry, &log).on_retryable(
            ::angzarr_embedded::next::OnRetryable {
                max_retries: 0,
                ..::angzarr_embedded::next::OnRetryable::default()
            },
        );
        let sampled = Sampled {
            now: at(9),
            command_id: Uuid::from_bytes([0x79; 16]),
        };

        let origin = ready(kernel.handle_any(
            &store,
            send("remote", 3, any(Note {}), None),
            &sampled,
            &mut (),
        ))
        .err()
        .expect("unreachable");
        assert!(
            matches!(
                &origin,
                Failure::Kernel(KernelError::HandlerUnavailable { cause, .. }) if cause == "not-running"
            ),
            "{origin:?}"
        );
        assert_eq!(origin.recovery(), Recovery::Retry);
        assert_eq!(origin.code(), "angzarr-embedded-next-handler-unavailable");

        handle_in::<Wallet>(
            &store,
            &registry,
            &log,
            1,
            send("wallet", 1, any(Open { bonus: 50 }), None),
        )
        .unwrap();
        let held = handle_in::<Wallet>(
            &store,
            &registry,
            &log,
            2,
            send("wallet", 2, any(Hold { cents: 5 }), None),
        )
        .err()
        .expect("the policy's step is unreachable");
        assert!(
            matches!(
                &held,
                Failure::Downstream {
                    error: StepError::Kernel(KernelError::HandlerUnavailable { .. }),
                    ..
                }
            ),
            "{held:?}"
        );
        assert_eq!(held.recovery(), Recovery::Retry);
        assert!(log.lines().is_empty(), "retryable: nothing dead-lettered");
        let remote_cover = Cover {
            domain: "remote".to_string(),
            root: Some(root()),
            edition: None,
        };
        assert_eq!(pages(&store, &remote_cover), None);

        up.store(true, std::sync::atomic::Ordering::SeqCst);
        ready(kernel.resume(&store, &sampled, &mut ())).unwrap();
        assert_eq!(
            pages(&store, &remote_cover).map(|p| p.len()),
            Some(1),
            "resumed by position once reachable"
        );
    }

    /// A remote handler down for its first `down_for` dispatches
    /// (COORDINATOR_REASON_HANDLER_UNAVAILABLE, retryable), then up; `calls` counts
    /// every dispatch.
    struct Recovering {
        down_for: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    impl ::angzarr_embedded::next::angzarr_router::router::CommandHandler for Recovering {
        fn name(&self) -> &str {
            "remote"
        }

        fn command_types(&self) -> Vec<String> {
            Remote.command_types()
        }

        async fn dispatch(
            &self,
            req: &pb::ContextualCommand,
        ) -> Result<
            pb::CommandHandlerResponse,
            ::angzarr_embedded::next::angzarr_router::error::CodedError,
        > {
            use std::sync::atomic::Ordering::SeqCst;
            self.calls.fetch_add(1, SeqCst);
            let down = self
                .down_for
                .fetch_update(SeqCst, SeqCst, |n| n.checked_sub(1))
                .is_ok();
            let up = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(!down));
            Flaky { up }.dispatch(req).await
        }
    }

    /// The backoffs the kernel waited out, in order, each completing at
    /// once; every jitter draw is `draw`.
    struct Waits {
        waited: std::sync::Mutex<Vec<std::time::Duration>>,
        draw: f64,
    }

    impl Waits {
        fn drawing(draw: f64) -> Waits {
            Waits {
                waited: std::sync::Mutex::new(Vec::new()),
                draw,
            }
        }

        /// Each wait, in microseconds, rounded.
        fn micros(&self) -> Vec<u128> {
            self.waited
                .lock()
                .unwrap()
                .iter()
                .map(|d| (d.as_nanos() + 500) / 1_000)
                .collect()
        }
    }

    impl ::angzarr_embedded::next::Wait for Waits {
        fn wait(
            &self,
            backoff: std::time::Duration,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
            self.waited.lock().unwrap().push(backoff);
            Box::pin(std::future::ready(()))
        }

        fn draw(&self) -> f64 {
            self.draw
        }
    }

    /// Retries 3 times, from 10 ms, tripling, at most 50 ms apart, up to
    /// half of each wait taken off at random.
    fn capped() -> ::angzarr_embedded::next::OnRetryable {
        ::angzarr_embedded::next::OnRetryable {
            max_retries: 3,
            max_race_runs: 10,
            initial_backoff: std::time::Duration::from_millis(10),
            multiplier: 3.0,
            max_backoff: std::time::Duration::from_millis(50),
            jitter: 0.5,
        }
    }

    /// The wallets' registry, with `remote` as the remote domain, and a
    /// policy sending it a `Note` on each wallet's `Held`.
    fn remote_registry(
        remote: Box<dyn ::angzarr_embedded::next::angzarr_router::router::DynCommandHandler>,
    ) -> Registry {
        let mut registry = registry();
        registry
            .command_handler("remote", remote, remote_descriptor())
            .unwrap();
        registry.policy_dispatch(
            PolicyDispatch::new("wallet-remote", "wallet", "remote").on_event(
                "toy.wallet.v1.Held",
                |_, source| {
                    Ok(vec![pb::CommandBook {
                        cover: Some(pb::Cover {
                            domain: "remote".to_string(),
                            root: source.and_then(|c| c.root.clone()),
                            edition: None,
                        }),
                        page: Some(pb::CommandPage {
                            body: Some(pb::command_page::Body::Command(pb::Command {
                                payload: Some(any(Note {})),
                            })),
                            ..Default::default()
                        }),
                        handling: None,
                    }])
                },
            ),
        );
        registry
    }

    /// [`remote_registry`] with [`Recovering`], down for `down_for`
    /// dispatches; and its dispatch count.
    fn recovering(down_for: usize) -> (Registry, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let registry = remote_registry(Box::new(Recovering {
            down_for: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(down_for)),
            calls: std::sync::Arc::clone(&calls),
        }));
        (registry, calls)
    }

    /// The application's instant and minted id `n`.
    fn sampled_at(n: u8) -> Sampled {
        Sampled {
            now: at(i64::from(n)),
            command_id: Uuid::from_bytes([n; 16]),
        }
    }

    /// Retrying is on by default, with no config: three retries, from 100
    /// ms, doubling, at most 5 s apart, a fifth of each wait at random.
    #[test]
    fn retrying_is_on_by_default() {
        use ::angzarr_embedded::next::OnRetryable;
        use std::time::Duration;
        let registry = registry();
        let log = MemoryDeadLetters::new();
        let kernel = ::angzarr_embedded::next::Kernel::new(&registry, &log);
        assert_eq!(
            kernel.retryable(),
            OnRetryable {
                max_retries: 3,
                max_race_runs: 10,
                initial_backoff: Duration::from_millis(100),
                multiplier: 2.0,
                max_backoff: Duration::from_secs(5),
                jitter: 0.2,
            }
        );
    }

    /// A retryable failure (decision 14.37: neither a business refusal nor
    /// a bug) is retried with exponential backoff (10, 30, then 50 ms, the
    /// most, not 90), each wait less its jitter (a draw of 0.5 at a jitter
    /// of 0.5 takes a quarter off), as many times as the cap allows, then
    /// handed back to its caller as last answered. The caller is waiting,
    /// so nothing is dead-lettered (owner ruling, 2026-10-31: only work
    /// with no caller left is; a policy's step is, below).
    #[test]
    fn a_retryable_failure_is_retried_with_backoff_and_jitter_then_handed_back() {
        use std::sync::atomic::Ordering::SeqCst;
        let (registry, calls) = recovering(usize::MAX);
        let store = Mem::new();
        let log = MemoryDeadLetters::new();
        let waits = Waits::drawing(0.5);
        let kernel = ::angzarr_embedded::next::Kernel::new(&registry, &log)
            .on_retryable(capped())
            .wait_with(&waits);
        let failed = ready(kernel.handle_any(
            &store,
            send("remote", 3, any(Note {}), None),
            &sampled_at(9),
            &mut (),
        ))
        .err()
        .expect("still unreachable after the cap");
        assert!(
            matches!(
                &failed,
                Failure::Kernel(KernelError::HandlerUnavailable { .. })
            ),
            "{failed:?}"
        );
        assert_eq!(calls.load(SeqCst), 4, "the first attempt and 3 retries");
        assert_eq!(waits.micros(), [7_500, 22_500, 37_500]);
        assert!(log.lines().is_empty(), "{:?}", log.lines());
    }

    /// `max_retries = 0` turns retrying off: a retryable failure goes back
    /// to the caller at once, waits for nothing and is not dead-lettered;
    /// as a policy's step, its position holds, so a resume runs it again.
    #[test]
    fn no_retries_turns_retrying_off() {
        use std::sync::atomic::Ordering::SeqCst;
        let (registry, calls) = recovering(usize::MAX);
        let store = Mem::new();
        let log = MemoryDeadLetters::new();
        let waits = Waits::drawing(0.0);
        let kernel = ::angzarr_embedded::next::Kernel::new(&registry, &log)
            .on_retryable(::angzarr_embedded::next::OnRetryable {
                max_retries: 0,
                ..capped()
            })
            .wait_with(&waits);
        let failed = ready(kernel.handle_any(
            &store,
            send("remote", 3, any(Note {}), None),
            &sampled_at(9),
            &mut (),
        ));
        assert!(failed.is_err());
        assert_eq!(calls.load(SeqCst), 1);
        assert!(waits.micros().is_empty());
        assert!(log.lines().is_empty(), "{:?}", log.lines());
    }

    /// As a policy's step, the same: retried up to the cap, then
    /// dead-lettered; the policy's position then does what `on_policy_bug`
    /// says: past the trigger by default (a resume does not run the step
    /// again), or at it under `Stay` (a resume does).
    #[test]
    fn a_policy_steps_retryable_failure_is_dead_lettered_after_the_cap() {
        use ::angzarr_embedded::next::{OnPolicyBug, StepError};
        use std::sync::atomic::Ordering::SeqCst;
        for (policy_bug, resumed_calls) in [(OnPolicyBug::MovePast, 4), (OnPolicyBug::Stay, 8)] {
            let (registry, calls) = recovering(usize::MAX);
            let store = Mem::new();
            let log = MemoryDeadLetters::new();
            let waits = Waits::drawing(0.0);
            let kernel = ::angzarr_embedded::next::Kernel::new(&registry, &log)
                .on_retryable(capped())
                .wait_with(&waits)
                .on_policy_bug(policy_bug);
            ready(kernel.handle_any(
                &store,
                send("wallet", 1, any(Open { bonus: 50 }), None),
                &sampled_at(1),
                &mut (),
            ))
            .unwrap();
            let held = ready(kernel.handle_any(
                &store,
                send("wallet", 2, any(Hold { cents: 5 }), None),
                &sampled_at(2),
                &mut (),
            ))
            .err()
            .expect("the policy's step is unreachable");
            assert!(
                matches!(
                    &held,
                    Failure::Downstream {
                        error: StepError::Kernel(KernelError::HandlerUnavailable { .. }),
                        ..
                    }
                ),
                "{held:?}"
            );
            assert_eq!(calls.load(SeqCst), 4, "{policy_bug:?}");
            assert_eq!(waits.micros(), [10_000, 30_000, 50_000], "{policy_bug:?}");
            let lines = log.lines();
            assert_eq!(lines.len(), 1, "{policy_bug:?}: {lines:?}");
            assert!(
                lines[0].contains("remote: COORDINATOR_REASON_HANDLER_UNAVAILABLE cause=not-running domain=remote retry_count=3"),
                "{lines:?}"
            );
            let _ = ready(kernel.resume(&store, &sampled_at(3), &mut ()));
            assert_eq!(calls.load(SeqCst), resumed_calls, "{policy_bug:?}");
        }
    }

    /// A retryable failure that clears within the cap writes nothing extra:
    /// no dead letter, the step's events once, and only the backoffs it
    /// needed.
    #[test]
    fn a_success_within_the_cap_writes_nothing_extra() {
        use std::sync::atomic::Ordering::SeqCst;
        let (registry, calls) = recovering(2);
        let store = Mem::new();
        let log = MemoryDeadLetters::new();
        let waits = Waits::drawing(0.0);
        let kernel = ::angzarr_embedded::next::Kernel::new(&registry, &log)
            .on_retryable(capped())
            .wait_with(&waits);
        let handled = ready(kernel.handle_any(
            &store,
            send("remote", 3, any(Note {}), None),
            &sampled_at(9),
            &mut (),
        ));
        assert!(
            matches!(handled, Ok(Handled::Applied(_))),
            "{:?}",
            handled.err()
        );
        assert_eq!(calls.load(SeqCst), 3);
        assert_eq!(waits.micros(), [10_000, 30_000]);
        assert!(log.lines().is_empty(), "{:?}", log.lines());
        let remote_cover = Cover {
            domain: "remote".to_string(),
            root: Some(root()),
            edition: None,
        };
        assert_eq!(pages(&store, &remote_cover).map(|p| p.len()), Some(1));
    }

    /// A remote handler that fails every command with `error`, counting
    /// its dispatches.
    struct Counted {
        error: ::angzarr_embedded::next::angzarr_router::error::CodedError,
        calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    impl ::angzarr_embedded::next::angzarr_router::router::CommandHandler for Counted {
        fn name(&self) -> &str {
            "remote"
        }

        fn command_types(&self) -> Vec<String> {
            Remote.command_types()
        }

        async fn dispatch(
            &self,
            _: &pb::ContextualCommand,
        ) -> Result<
            pb::CommandHandlerResponse,
            ::angzarr_embedded::next::angzarr_router::error::CodedError,
        > {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Err(self.error.clone())
        }
    }

    /// [`remote_registry`] with [`Counted`] failing every command under its
    /// own `code` and status `grpc`; and its dispatch count.
    fn counted(
        code: &str,
        grpc: ::angzarr_embedded::next::angzarr_router::error::GrpcCode,
    ) -> (Registry, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let registry = remote_registry(Box::new(Counted {
            error: ::angzarr_embedded::next::angzarr_router::error::CodedError {
                code: code.to_string(),
                extras: Default::default(),
                grpc,
                refusal: None,
            },
            calls: std::sync::Arc::clone(&calls),
        }));
        (registry, calls)
    }

    /// ABORTED (a stale or overlapping command, or a handler's own choice)
    /// is a business answer to return at once to an outside caller, to
    /// reload: never retried, never dead-lettered (the owner's ruling,
    /// 2026-10-29).
    #[test]
    fn an_aborted_answer_goes_back_to_an_outside_caller_at_once() {
        use ::angzarr_embedded::next::angzarr_router::error::GrpcCode;
        use std::sync::atomic::Ordering::SeqCst;
        let (registry, calls) = counted("SEAT_MOVED", GrpcCode::Aborted);
        let store = Mem::new();
        let log = MemoryDeadLetters::new();
        let waits = Waits::drawing(0.0);
        let kernel = ::angzarr_embedded::next::Kernel::new(&registry, &log)
            .on_retryable(capped())
            .wait_with(&waits);
        let failed = ready(kernel.handle_any(
            &store,
            send("remote", 3, any(Note {}), None),
            &sampled_at(9),
            &mut (),
        ));
        assert!(failed.is_err());
        assert_eq!(calls.load(SeqCst), 1, "not retried");
        assert!(waits.micros().is_empty());
        assert!(log.lines().is_empty(), "{:?}", log.lines());
    }

    /// As a policy's step, ABORTED has the kernel for its caller: it
    /// reloads and runs the step again under the capped backoff, then
    /// dead-letters it.
    #[test]
    fn a_policy_steps_aborted_answer_is_retried_then_dead_lettered() {
        use ::angzarr_embedded::next::angzarr_router::error::GrpcCode;
        use std::sync::atomic::Ordering::SeqCst;
        let (registry, calls) = counted("SEAT_MOVED", GrpcCode::Aborted);
        let store = Mem::new();
        let log = MemoryDeadLetters::new();
        let waits = Waits::drawing(0.0);
        let kernel = ::angzarr_embedded::next::Kernel::new(&registry, &log)
            .on_retryable(capped())
            .wait_with(&waits);
        ready(kernel.handle_any(
            &store,
            send("wallet", 1, any(Open { bonus: 50 }), None),
            &sampled_at(1),
            &mut (),
        ))
        .unwrap();
        let held = ready(kernel.handle_any(
            &store,
            send("wallet", 2, any(Hold { cents: 5 }), None),
            &sampled_at(2),
            &mut (),
        ));
        assert!(held.is_err());
        assert_eq!(calls.load(SeqCst), 4, "the first attempt and 3 retries");
        assert_eq!(waits.micros(), [10_000, 30_000, 50_000]);
        let lines = log.lines();
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert!(lines[0].contains("retry_count=3"), "{lines:?}");
        // A policy step's ABORTED re-run at its cap records the handler's
        // own key as last answered: processing_failed, its retries,
        // transient (decision 14.48).
        let reason = crate::next::toys::wire("SEAT_MOVED", &[]);
        crate::next::toys::assert_letter(
            &crate::next::toys::one_letter(log.letters()),
            ::angzarr_embedded::next::v1::ComponentKind::Aggregate,
            &reason,
            crate::next::toys::processing_failed(&reason, 3, true),
        );
    }

    /// A bug of the fatal kind (INTERNAL, UNKNOWN, DATA_LOSS) is
    /// dead-lettered at once, never retried; any other status that is no
    /// refusal and not ABORTED (UNAVAILABLE, UNIMPLEMENTED and the rest)
    /// is a retryable bug: retried, then handed back to its waiting
    /// caller, not dead-lettered.
    #[test]
    fn a_fatal_bug_is_never_retried_and_a_retryable_one_is() {
        use ::angzarr_embedded::next::angzarr_router::error::GrpcCode;
        use std::sync::atomic::Ordering::SeqCst;
        for (grpc, attempts, letters) in [
            (GrpcCode::Internal, 1, 1),
            (GrpcCode::Unknown, 1, 1),
            (GrpcCode::DataLoss, 1, 1),
            (GrpcCode::Unavailable, 4, 0),
            (GrpcCode::Unimplemented, 4, 0),
        ] {
            let (registry, calls) = counted("PLAYER_BROKE", grpc);
            let store = Mem::new();
            let log = MemoryDeadLetters::new();
            let waits = Waits::drawing(0.0);
            let kernel = ::angzarr_embedded::next::Kernel::new(&registry, &log)
                .on_retryable(capped())
                .wait_with(&waits);
            let failed = ready(kernel.handle_any(
                &store,
                send("remote", 3, any(Note {}), None),
                &sampled_at(9),
                &mut (),
            ));
            assert!(failed.is_err(), "{grpc:?}");
            assert_eq!(calls.load(SeqCst), attempts, "{grpc:?}");
            assert_eq!(log.lines().len(), letters, "{grpc:?}: {:?}", log.lines());
        }
    }

    /// A marked-up handler's retryable bug and immediate return
    /// (`Failed::retryable_bug`, `Failed::immediate`), through the kernel
    /// as an application runs it: the meter's [`Meter::probe`] and
    /// [`Meter::seal`], their dependency [`PROBE`].
    mod marked_up {
        use super::*;
        use ::angzarr_embedded::next::angzarr_router::error::GrpcCode;

        fn meter_cover() -> Cover {
            Cover {
                domain: "meter".to_string(),
                root: Some(root()),
                edition: None,
            }
        }

        /// The router's error `failure` is, at the origin or down the
        /// chain.
        fn coded<R: fmt::Debug>(
            failure: &Failure<R>,
        ) -> &::angzarr_embedded::next::angzarr_router::error::CodedError {
            match failure {
                Failure::Kernel(KernelError::Router(error))
                | Failure::Downstream {
                    error: StepError::Kernel(KernelError::Router(error)),
                    ..
                } => error,
                other => panic!("the router's error, not {other:?}"),
            }
        }

        /// A probe of `status`, the handler's own (or, with `step`, its
        /// step's).
        fn probe(status: i64, step: bool) -> Any {
            any(Probe { status, step })
        }

        /// The capped kernel over `registry`, waiting with `waits`, its
        /// dead letters to `log`, handling command `n` to the meter.
        fn handled(
            registry: &Registry,
            store: &Mem,
            log: &MemoryDeadLetters,
            waits: &Waits,
            n: u8,
            payload: Any,
        ) -> Result<Handled, Failure<Rejected>> {
            let kernel = ::angzarr_embedded::next::Kernel::new(registry, log)
                .on_retryable(capped())
                .wait_with(waits);
            ready(kernel.handle_any(
                store,
                send("meter", n, payload, None),
                &sampled_at(n),
                &mut (),
            ))
        }

        /// A handler's retryable bug is retried up to the cap, each wait
        /// the backoff less its jitter, through the injected `Wait`; then
        /// the caller gets it as ROUTER_REASON_UNHANDLED_HANDLER_ERROR with
        /// its cause, under its status, `Recovery::Retry`. The caller is
        /// waiting, so nothing is dead-lettered, and nothing is written.
        #[test]
        fn a_handlers_retryable_bug_is_retried_to_the_cap_then_handed_back() {
            let registry = registry();
            let store = Mem::new();
            let log = MemoryDeadLetters::new();
            let waits = Waits::drawing(0.5);

            let failed = handled(&registry, &store, &log, &waits, 3, probe(1, false))
                .err()
                .expect("still down after the cap");

            let error = coded(&failed);
            assert_eq!(
                error.code, "ROUTER_REASON_UNHANDLED_HANDLER_ERROR",
                "{failed:?}"
            );
            assert_eq!(error.grpc, GrpcCode::Unavailable, "{failed:?}");
            assert_eq!(failed.recovery(), Recovery::Retry);
            assert_eq!(probe_attempts(), 4, "the first attempt and 3 retries");
            assert_eq!(waits.micros(), [7_500, 22_500, 37_500]);
            assert!(log.lines().is_empty(), "{:?}", log.lines());
            assert_eq!(pages(&store, &meter_cover()), None, "nothing written");
        }

        /// A retryable bug that clears within the cap writes once and
        /// nothing extra: the decision's event, no dead letter, only the
        /// backoffs it needed. A step's, the same.
        #[test]
        fn a_success_within_the_cap_writes_once_and_nothing_extra() {
            for step in [false, true] {
                probe_down_for(2);
                let registry = registry();
                let store = Mem::new();
                let log = MemoryDeadLetters::new();
                let waits = Waits::drawing(0.0);

                let handled = handled(&registry, &store, &log, &waits, 3, probe(2, step));

                assert!(
                    matches!(handled, Ok(Handled::Applied(_))),
                    "step {step}: {:?}",
                    handled.err()
                );
                assert_eq!(probe_attempts(), 3, "step {step}");
                assert_eq!(waits.micros(), [10_000, 30_000], "step {step}");
                assert!(log.lines().is_empty(), "step {step}: {:?}", log.lines());
                assert_eq!(
                    pages(&store, &meter_cover()).map(|p| p.len()),
                    Some(1),
                    "step {step}: written once"
                );
            }
        }

        /// An immediate return (a handler's, a step's, a typed-refusal
        /// handler's) goes back to an outside caller at once as ABORTED
        /// under its own key and args: never retried, nothing waited,
        /// nothing dead-lettered, nothing written, `Recovery::Retry`.
        #[test]
        fn an_immediate_return_goes_back_to_a_caller_at_once_under_its_key() {
            for (payload, seat) in [
                (probe(4, false), "5"),
                (probe(4, true), "6"),
                (any(Seal { code: 104 }), "9"),
            ] {
                probe_down_for(u32::MAX);
                let registry = registry();
                let store = Mem::new();
                let log = MemoryDeadLetters::new();
                let waits = Waits::drawing(0.0);

                let failed = handled(&registry, &store, &log, &waits, 3, payload)
                    .err()
                    .expect("moved");

                let error = coded(&failed);
                assert_eq!(error.grpc, GrpcCode::Aborted, "{failed:?}");
                assert_eq!(error.code, "toy-meter-moved", "{failed:?}");
                assert_eq!(
                    error.extras.get("seat").map(String::as_str),
                    Some(seat),
                    "{failed:?}"
                );
                assert_eq!(failed.recovery(), Recovery::Retry);
                assert_eq!(probe_attempts(), 1, "not retried: {failed:?}");
                assert!(waits.micros().is_empty(), "{failed:?}");
                assert!(log.lines().is_empty(), "{:?}", log.lines());
                assert_eq!(pages(&store, &meter_cover()), None, "nothing written");
            }
        }

        /// The wallets' registry with a policy probing the meter (`status`)
        /// on each wallet's `Held`.
        fn probing(status: i64) -> Registry {
            let mut registry = registry();
            registry.policy_dispatch(
                PolicyDispatch::new("wallet-meter", "wallet", "meter").on_event(
                    "toy.wallet.v1.Held",
                    move |_, source| {
                        Ok(vec![pb::CommandBook {
                            cover: Some(pb::Cover {
                                domain: "meter".to_string(),
                                root: source.and_then(|c| c.root.clone()),
                                edition: None,
                            }),
                            page: Some(pb::CommandPage {
                                body: Some(pb::command_page::Body::Command(pb::Command {
                                    payload: Some(any(Probe {
                                        status,
                                        step: false,
                                    })),
                                })),
                                ..Default::default()
                            }),
                            handling: None,
                        }])
                    },
                ),
            );
            registry
        }

        /// As a policy's step, a handler's retryable bug and its immediate
        /// return alike have the kernel for their caller: the step is
        /// reloaded and run again under the capped backoff, then
        /// dead-lettered with its cause and retry count.
        #[test]
        fn a_policy_steps_retryable_bug_or_immediate_return_is_rerun_then_dead_lettered() {
            for (status, cause) in [
                (
                    1,
                    ": ROUTER_REASON_UNHANDLED_HANDLER_ERROR cause=toy-meter-broke cause.seat=5",
                ),
                (4, ": toy-meter-moved seat=5"),
            ] {
                probe_down_for(u32::MAX);
                let registry = probing(status);
                let store = Mem::new();
                let log = MemoryDeadLetters::new();
                let waits = Waits::drawing(0.0);
                let kernel = ::angzarr_embedded::next::Kernel::new(&registry, &log)
                    .on_retryable(capped())
                    .wait_with(&waits);
                ready(kernel.handle_any(
                    &store,
                    send("wallet", 1, any(Open { bonus: 50 }), None),
                    &sampled_at(1),
                    &mut (),
                ))
                .unwrap();

                let held = ready(kernel.handle_any(
                    &store,
                    send("wallet", 2, any(Hold { cents: 5 }), None),
                    &sampled_at(2),
                    &mut (),
                ))
                .err()
                .expect("the policy's step failed");

                assert!(
                    matches!(held, Failure::Downstream { .. }),
                    "status {status}: {held:?}"
                );
                assert_eq!(probe_attempts(), 4, "status {status}");
                assert_eq!(waits.micros(), [10_000, 30_000, 50_000], "status {status}");
                let lines = log.lines();
                assert_eq!(lines.len(), 1, "status {status}: {lines:?}");
                assert!(lines[0].contains(cause), "status {status}: {lines:?}");
                assert!(
                    lines[0].contains("retry_count=3"),
                    "status {status}: {lines:?}"
                );
                assert_eq!(pages(&store, &meter_cover()), None, "status {status}");
            }
        }

        /// Each `RetryableStatus` a handler picks goes out exactly as that
        /// status, from a handler, a step and a typed-refusal handler
        /// alike: ROUTER_REASON_UNHANDLED_HANDLER_ERROR, its cause beside.
        #[test]
        fn each_retryable_status_goes_out_exactly() {
            for (n, status) in [
                (1, RetryableStatus::Unavailable),
                (2, RetryableStatus::DeadlineExceeded),
                (3, RetryableStatus::ResourceExhausted),
            ] {
                for (payload, seat) in [
                    (probe(n, false), "5"),
                    (probe(n, true), "6"),
                    (any(Seal { code: 100 + n }), "9"),
                ] {
                    let store = Mem::new();
                    let log = MemoryDeadLetters::new();

                    let failed = handle_in::<Meter>(
                        &store,
                        &registry(),
                        &log,
                        1,
                        send("meter", 1, payload, None),
                    )
                    .err()
                    .expect("down");

                    let error = coded(&failed);
                    assert_eq!(error.grpc, GrpcCode::from(status), "{status:?}: {failed:?}");
                    assert_eq!(
                        error.code, "ROUTER_REASON_UNHANDLED_HANDLER_ERROR",
                        "{status:?}"
                    );
                    assert_eq!(
                        error.extras.get("cause").map(String::as_str),
                        Some("toy-meter-broke"),
                        "{status:?}"
                    );
                    assert_eq!(
                        error.extras.get("cause.seat").map(String::as_str),
                        Some(seat),
                        "{status:?}"
                    );
                }
            }
        }
    }

    /// A caller's own `expected_sequence` ahead of the head goes back to
    /// the caller at once, malformed (decision 14.47): never retried,
    /// never dead-lettered.
    #[test]
    fn a_callers_expected_sequence_ahead_goes_back_at_once() {
        let (registry, _) = recovering(0);
        let store = Mem::new();
        let log = MemoryDeadLetters::new();
        let waits = Waits::drawing(0.0);
        let kernel = ::angzarr_embedded::next::Kernel::new(&registry, &log)
            .on_retryable(capped())
            .wait_with(&waits);
        let mut stale = send("remote", 3, any(Note {}), None);
        stale.handling = Some(::angzarr_embedded::next::v1::Handling {
            expected_sequence: Some(5),
            conflict_resolution: ConflictResolution::Strict as i32,
        });
        let failed = ready(kernel.handle_any(&store, stale, &sampled_at(9), &mut ()))
            .err()
            .expect("ahead");
        assert!(
            matches!(
                &failed,
                Failure::Kernel(KernelError::ExpectedSequenceAhead {
                    expected: 5,
                    head: 0,
                    ..
                })
            ),
            "{failed:?}"
        );
        assert!(waits.micros().is_empty(), "{:?}", waits.micros());
        assert!(log.lines().is_empty(), "{:?}", log.lines());
    }

    /// A remote handler that fails every command with `error`.
    struct Faulty {
        error: ::angzarr_embedded::next::angzarr_router::error::CodedError,
    }

    impl ::angzarr_embedded::next::angzarr_router::router::CommandHandler for Faulty {
        fn name(&self) -> &str {
            "remote"
        }

        fn command_types(&self) -> Vec<String> {
            Remote.command_types()
        }

        async fn dispatch(
            &self,
            _: &pb::ContextualCommand,
        ) -> Result<
            pb::CommandHandlerResponse,
            ::angzarr_embedded::next::angzarr_router::error::CodedError,
        > {
            Err(self.error.clone())
        }
    }

    /// A registry with [`Faulty`] answering `grpc` under a code of its own,
    /// one the router does not know (`PLAYER_BROKE`), as the remote domain,
    /// and a policy sending it a `Note` on each wallet's `Held`.
    fn faulty(grpc: ::angzarr_embedded::next::angzarr_router::error::GrpcCode) -> Registry {
        faulty_under("PLAYER_BROKE", grpc)
    }

    /// [`faulty`], its handler failing under `code` (with a `seat` arg).
    fn faulty_under(
        code: &'static str,
        grpc: ::angzarr_embedded::next::angzarr_router::error::GrpcCode,
    ) -> Registry {
        faulty_with(
            ::angzarr_embedded::next::angzarr_router::error::CodedError {
                code: code.to_string(),
                extras: [("seat".to_string(), "3".to_string())].into(),
                grpc,
                refusal: None,
            },
        )
    }

    /// [`faulty`], its handler failing with `error`.
    fn faulty_with(error: ::angzarr_embedded::next::angzarr_router::error::CodedError) -> Registry {
        use ::angzarr_embedded::next::angzarr_router::router::DynCommandHandler;
        let mut registry = registry();
        let faulty: Box<dyn DynCommandHandler> = Box::new(Faulty { error });
        registry
            .command_handler("remote", faulty, remote_descriptor())
            .unwrap();
        registry.policy_dispatch(
            PolicyDispatch::new("wallet-remote", "wallet", "remote").on_event(
                "toy.wallet.v1.Held",
                |_, source| {
                    Ok(vec![pb::CommandBook {
                        cover: Some(pb::Cover {
                            domain: "remote".to_string(),
                            root: source.and_then(|c| c.root.clone()),
                            edition: None,
                        }),
                        page: Some(pb::CommandPage {
                            body: Some(pb::command_page::Body::Command(pb::Command {
                                payload: Some(any(Note {})),
                            })),
                            ..Default::default()
                        }),
                        handling: None,
                    }])
                },
            ),
        );
        registry
    }

    /// A code the router does not know, refused FAILED_PRECONDITION, is the
    /// domain's deliberate refusal: a business rejection under its own
    /// code and args, nothing dead-lettered.
    #[test]
    fn an_unknown_code_refused_as_failed_precondition_is_a_rejection() {
        use ::angzarr_embedded::next::angzarr_router::error::GrpcCode;
        refused_as_a_rejection(GrpcCode::FailedPrecondition);
    }

    /// A code the router does not know, refused INVALID_ARGUMENT, is a
    /// business refusal too (the owner's ruling, 2026-10-29).
    #[test]
    fn an_unknown_code_refused_as_invalid_argument_is_a_rejection() {
        use ::angzarr_embedded::next::angzarr_router::error::GrpcCode;
        refused_as_a_rejection(GrpcCode::InvalidArgument);
    }

    /// A code the router does not know, refused NOT_FOUND, is a business
    /// refusal too (the owner's ruling, 2026-10-29).
    #[test]
    fn an_unknown_code_refused_as_not_found_is_a_rejection() {
        use ::angzarr_embedded::next::angzarr_router::error::GrpcCode;
        refused_as_a_rejection(GrpcCode::NotFound);
    }

    /// A remote handler refusing under a code of its own with `grpc` is a
    /// business rejection under that code and args, nothing dead-lettered.
    fn refused_as_a_rejection(grpc: ::angzarr_embedded::next::angzarr_router::error::GrpcCode) {
        let registry = faulty(grpc);
        let store = Mem::new();
        let log = MemoryDeadLetters::new();
        let kernel = ::angzarr_embedded::next::Kernel::new(&registry, &log);
        let sampled = Sampled {
            now: at(9),
            command_id: Uuid::from_bytes([0x79; 16]),
        };
        let refused = ready(kernel.handle_any(
            &store,
            send("remote", 3, any(Note {}), None),
            &sampled,
            &mut (),
        ))
        .err()
        .expect("refused");
        let Failure::Rejected(rejected) = &refused else {
            panic!("{grpc:?}, a business rejection: {refused:?}");
        };
        assert_eq!(rejected.code(), "PLAYER_BROKE");
        assert!(
            matches!(rejected.args().as_slice(), [("seat", Arg::Str(v))] if v == "3"),
            "its args: {rejected:?}"
        );
        assert_eq!(refused.recovery(), Recovery::Business);
        assert!(log.lines().is_empty(), "{:?}", log.lines());
    }

    /// A code the router does not know, failed with a fatal bug's status
    /// (INTERNAL, UNKNOWN, DATA_LOSS), is a bug, never a rejection: the
    /// router's error, `Recovery::Bug`,
    /// dead-lettered by its code and args (no English), at the origin and
    /// as a policy's step, whose position moves past the trigger, as for a
    /// policy's own error: the dead letter is the record to replay, so a
    /// resume does not run it again.
    #[test]
    fn an_unknown_code_failed_under_a_fatal_status_is_a_dead_lettered_bug() {
        use ::angzarr_embedded::next::angzarr_router::error::GrpcCode;
        use ::angzarr_embedded::next::StepError;
        for grpc in [GrpcCode::Internal, GrpcCode::Unknown, GrpcCode::DataLoss] {
            let registry = faulty(grpc);
            let store = Mem::new();
            let log = MemoryDeadLetters::new();
            let kernel = ::angzarr_embedded::next::Kernel::new(&registry, &log);
            let sampled = Sampled {
                now: at(9),
                command_id: Uuid::from_bytes([0x79; 16]),
            };
            let dead_lettered = |lines: &[String]| {
                assert_eq!(lines.len(), 1, "{grpc:?}: {lines:?}");
                assert!(
                    lines[0].starts_with(::angzarr_embedded::next::DEAD_LETTER_PREFIX),
                    "{}",
                    lines[0]
                );
                assert!(
                    lines[0].contains("remote: PLAYER_BROKE seat=3"),
                    "{grpc:?}, its own key and args, as answered: {}",
                    lines[0]
                );
                assert!(!lines[0].contains("broke"), "no English: {}", lines[0]);
                assert!(
                    lines[0].contains(" command_id="),
                    "the command: {}",
                    lines[0]
                );
            };

            let origin = ready(kernel.handle_any(
                &store,
                send("remote", 3, any(Note {}), None),
                &sampled,
                &mut (),
            ))
            .err()
            .expect("failed");
            assert!(
                matches!(
                    &origin,
                    Failure::Kernel(KernelError::Router(e)) if e.code == "PLAYER_BROKE"
                ),
                "{grpc:?}: {origin:?}"
            );
            assert_eq!(origin.recovery(), Recovery::Bug, "{grpc:?}");
            dead_lettered(&log.lines());
            assert!(
                log.lines()[0].contains(&Uuid::from_bytes([3; 16]).to_string()),
                "the origin's command id: {:?}",
                log.lines()
            );

            handle_in::<Wallet>(
                &store,
                &registry,
                &log,
                1,
                send("wallet", 1, any(Open { bonus: 50 }), None),
            )
            .unwrap();
            let log = MemoryDeadLetters::new();
            let held = handle_in::<Wallet>(
                &store,
                &registry,
                &log,
                2,
                send("wallet", 2, any(Hold { cents: 5 }), None),
            )
            .err()
            .expect("the policy's step fails");
            assert!(
                matches!(
                    &held,
                    Failure::Downstream {
                        error: StepError::Kernel(KernelError::Router(e)),
                        ..
                    } if e.code == "PLAYER_BROKE"
                ),
                "{grpc:?}: {held:?}"
            );
            assert_eq!(held.recovery(), Recovery::Bug, "{grpc:?}");
            dead_lettered(&log.lines());

            let log = MemoryDeadLetters::new();
            let kernel = ::angzarr_embedded::next::Kernel::new(&registry, &log);
            let resumed = ready(kernel.resume(&store, &sampled, &mut ()))
                .expect("{grpc:?}: nothing left owed");
            assert!(
                resumed.steps().is_empty(),
                "{grpc:?}: the position moved past the trigger"
            );
            assert!(log.lines().is_empty(), "{grpc:?}: {:?}", log.lines());
        }
    }

    /// Under `OnPolicyBug::Stay`, a handler's bug in a policy's step is
    /// still dead-lettered, but the policy's position stays at the
    /// trigger, so a resume runs the step again and dead-letters it again.
    #[test]
    fn under_stay_a_handlers_bug_in_a_policys_step_holds_its_position() {
        use ::angzarr_embedded::next::angzarr_router::error::GrpcCode;
        use ::angzarr_embedded::next::{Kernel, OnPolicyBug, StepError};
        let registry = faulty(GrpcCode::Internal);
        let store = Mem::new();
        let log = MemoryDeadLetters::new();
        let kernel = Kernel::new(&registry, &log).on_policy_bug(OnPolicyBug::Stay);
        let run = |n: u8, book| {
            let sampled = Sampled {
                now: at(i64::from(n)),
                command_id: Uuid::from_bytes([0x70 | n; 16]),
            };
            ready(kernel.handle::<Wallet, _, _>(&store, book, &sampled, &mut ()))
        };
        run(1, send("wallet", 1, any(Open { bonus: 50 }), None)).unwrap();
        let held = run(2, send("wallet", 2, any(Hold { cents: 5 }), None))
            .err()
            .expect("the policy's step fails");
        assert!(
            matches!(
                &held,
                Failure::Downstream {
                    error: StepError::Kernel(KernelError::Router(e)),
                    ..
                } if e.code == "PLAYER_BROKE"
            ),
            "{held:?}"
        );
        assert_eq!(log.lines().len(), 1, "{:?}", log.lines());
        let sampled = Sampled {
            now: at(9),
            command_id: Uuid::from_bytes([0x79; 16]),
        };
        assert!(
            ready(kernel.resume(&store, &sampled, &mut ())).is_err(),
            "the position stayed, so the step runs again"
        );
        assert_eq!(
            log.lines().len(),
            2,
            "dead-lettered again: {:?}",
            log.lines()
        );
    }

    /// A handler's bug (angzarr-router's `CodedError::bug`, decision
    /// 14.37) is INTERNAL ROUTER_REASON_UNHANDLED_HANDLER_ERROR with the handler's own
    /// code and args as its cause: `Recovery::Bug`, dead-lettered by that
    /// code and its cause (`cause=<code>`, `cause.<arg>`), at the origin and
    /// as a policy's step, whose position moves past the trigger.
    #[test]
    fn a_handlers_bug_is_dead_lettered_with_its_code_as_the_cause() {
        use ::angzarr_embedded::next::angzarr_router::error::CodedError;
        use ::angzarr_embedded::next::StepError;
        let registry = faulty_with(CodedError::bug(
            "PLAYER_BROKE",
            [("seat".to_string(), "3".to_string())],
        ));
        let store = Mem::new();
        let log = MemoryDeadLetters::new();
        let kernel = ::angzarr_embedded::next::Kernel::new(&registry, &log);
        let sampled = Sampled {
            now: at(9),
            command_id: Uuid::from_bytes([0x79; 16]),
        };
        let dead_lettered = |lines: &[String]| {
            assert_eq!(lines.len(), 1, "{lines:?}");
            assert!(
                lines[0].starts_with(::angzarr_embedded::next::DEAD_LETTER_PREFIX),
                "{}",
                lines[0]
            );
            assert!(
                lines[0].contains(
                    "remote: ROUTER_REASON_UNHANDLED_HANDLER_ERROR cause=PLAYER_BROKE cause.seat=3"
                ),
                "its key, and the handler's code and args as its cause: {}",
                lines[0]
            );
            assert!(!lines[0].contains("broke"), "no English: {}", lines[0]);
            assert!(
                lines[0].contains(" command_id="),
                "the command: {}",
                lines[0]
            );
        };

        let origin = ready(kernel.handle_any(
            &store,
            send("remote", 3, any(Note {}), None),
            &sampled,
            &mut (),
        ))
        .err()
        .expect("failed");
        assert!(
            matches!(
                &origin,
                Failure::Kernel(KernelError::Router(e)) if e.code == "ROUTER_REASON_UNHANDLED_HANDLER_ERROR"
            ),
            "{origin:?}"
        );
        assert_eq!(origin.recovery(), Recovery::Bug);
        dead_lettered(&log.lines());

        handle_in::<Wallet>(
            &store,
            &registry,
            &log,
            1,
            send("wallet", 1, any(Open { bonus: 50 }), None),
        )
        .unwrap();
        let log = MemoryDeadLetters::new();
        let held = handle_in::<Wallet>(
            &store,
            &registry,
            &log,
            2,
            send("wallet", 2, any(Hold { cents: 5 }), None),
        )
        .err()
        .expect("the policy's step fails");
        assert!(
            matches!(
                &held,
                Failure::Downstream {
                    error: StepError::Kernel(KernelError::Router(e)),
                    ..
                } if e.code == "ROUTER_REASON_UNHANDLED_HANDLER_ERROR"
            ),
            "{held:?}"
        );
        assert_eq!(held.recovery(), Recovery::Bug);
        dead_lettered(&log.lines());

        let log = MemoryDeadLetters::new();
        let kernel = ::angzarr_embedded::next::Kernel::new(&registry, &log);
        let resumed = ready(kernel.resume(&store, &sampled, &mut ())).expect("nothing left owed");
        assert!(
            resumed.steps().is_empty(),
            "the position moved past the trigger"
        );
        assert!(log.lines().is_empty(), "{:?}", log.lines());
    }

    /// A source whose fact handler fails with a fault of the router's own
    /// (ROUTER_REASON_UNHANDLED_HANDLER_ERROR): its commands answer as [`Remote`]'s.
    struct Picky;

    impl ::angzarr_embedded::next::angzarr_router::router::CommandHandler for Picky {
        fn name(&self) -> &str {
            "picky"
        }

        fn command_types(&self) -> Vec<String> {
            Remote.command_types()
        }

        async fn dispatch(
            &self,
            req: &pb::ContextualCommand,
        ) -> Result<
            pb::CommandHandlerResponse,
            ::angzarr_embedded::next::angzarr_router::error::CodedError,
        > {
            use ::angzarr_embedded::next::angzarr_router::error::CodedError;
            let fact = req
                .command
                .as_ref()
                .and_then(|c| c.page.as_ref())
                .is_some_and(|p| matches!(p.body, Some(pb::command_page::Body::Fact(_))));
            match fact {
                true => Err(CodedError::bug("PICKY_FACT_BROKE", [])),
                false => Remote.dispatch(req).await,
            }
        }
    }

    /// A registry with [`Picky`] as the source of a policy that sends the
    /// wallet a `Hold` it refuses on each `Noted`, and a store with the
    /// wallet opened.
    fn picky() -> (Registry, Mem) {
        picky_with(Box::new(Picky))
    }

    /// [`picky`], with `picky` as the source.
    fn picky_with(
        picky: Box<dyn ::angzarr_embedded::next::angzarr_router::router::DynCommandHandler>,
    ) -> (Registry, Mem) {
        let mut registry = registry();
        registry
            .command_handler("picky", picky, remote_descriptor())
            .unwrap();
        registry.policy_dispatch(
            PolicyDispatch::new("picky-wallet", "picky", "wallet").on_event(
                "toy.wallet.v1.Noted",
                |_, _| {
                    Ok(vec![pb::CommandBook {
                        cover: Some(wallet_cover("wallet")),
                        page: Some(pb::CommandPage {
                            body: Some(pb::command_page::Body::Command(pb::Command {
                                payload: Some(any(Hold { cents: 1000 })),
                            })),
                            ..Default::default()
                        }),
                        handling: None,
                    }])
                },
            ),
        );
        let store = Mem::new();
        let log = MemoryDeadLetters::new();
        handle_in::<Wallet>(
            &store,
            &registry,
            &log,
            1,
            send("wallet", 1, any(Open { bonus: 50 }), None),
        )
        .unwrap();
        (registry, store)
    }

    /// The refusal fact's step failing on [`Picky`]'s fact handler.
    fn assert_the_fact_step_failed<R: fmt::Debug>(failed: &Failure<R>) {
        use ::angzarr_embedded::next::StepError;
        assert!(
            matches!(
                failed,
                Failure::Downstream {
                    error: StepError::Kernel(KernelError::Router(e)),
                    ..
                } if e.code == "ROUTER_REASON_UNHANDLED_HANDLER_ERROR"
            ),
            "{failed:?}"
        );
    }

    /// A refusal fact its source fails on with a bug, not for having no
    /// handler for it, fails the step as that bug: dead-lettered as the
    /// source's handler's bug (never as an unhandled refusal), and the
    /// policy's position moves past the trigger.
    #[test]
    fn a_refusal_fact_its_source_fails_on_fails_the_step() {
        let (registry, store) = picky();
        let log = MemoryDeadLetters::new();
        let kernel = ::angzarr_embedded::next::Kernel::new(&registry, &log);
        let sampled = Sampled {
            now: at(9),
            command_id: Uuid::from_bytes([0x79; 16]),
        };
        let failed = ready(kernel.handle_any(
            &store,
            send("picky", 2, any(Note {}), None),
            &sampled,
            &mut (),
        ))
        .err()
        .expect("the refusal fact's step fails");
        assert_the_fact_step_failed(&failed);
        let lines = log.lines();
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert!(
            lines[0]
                .contains("picky: ROUTER_REASON_UNHANDLED_HANDLER_ERROR cause=PICKY_FACT_BROKE"),
            "the source's handler's bug, its code the cause: {}",
            lines[0]
        );

        let log = MemoryDeadLetters::new();
        let kernel = ::angzarr_embedded::next::Kernel::new(&registry, &log);
        let resumed = ready(kernel.resume(&store, &sampled, &mut ())).expect("nothing left owed");
        assert!(
            resumed.steps().is_empty(),
            "the position moved past the trigger"
        );
        assert!(log.lines().is_empty(), "{:?}", log.lines());
    }

    /// A source like [`Picky`] whose fact handler answers a code of its own
    /// under UNAVAILABLE, a retryable bug.
    struct Unsteady;

    impl ::angzarr_embedded::next::angzarr_router::router::CommandHandler for Unsteady {
        fn name(&self) -> &str {
            "picky"
        }

        fn command_types(&self) -> Vec<String> {
            Remote.command_types()
        }

        async fn dispatch(
            &self,
            req: &pb::ContextualCommand,
        ) -> Result<
            pb::CommandHandlerResponse,
            ::angzarr_embedded::next::angzarr_router::error::CodedError,
        > {
            use ::angzarr_embedded::next::angzarr_router::error::{CodedError, GrpcCode};
            let fact = req
                .command
                .as_ref()
                .and_then(|c| c.page.as_ref())
                .is_some_and(|p| matches!(p.body, Some(pb::command_page::Body::Fact(_))));
            match fact {
                true => Err(CodedError {
                    code: "PICKY_BUSY".to_string(),
                    extras: Default::default(),
                    grpc: GrpcCode::Unavailable,
                    refusal: None,
                }),
                false => Remote.dispatch(req).await,
            }
        }
    }

    /// A refusal fact its source fails on with a retryable bug is retried
    /// to the cap, then dead-lettered, and the policy's position moves past
    /// the trigger, as for a bug: a resume does not run it again.
    #[test]
    fn a_refusal_fact_its_source_cannot_take_yet_is_retried_then_moved_past() {
        let (registry, store) = picky_with(Box::new(Unsteady));
        let log = MemoryDeadLetters::new();
        let waits = Waits::drawing(0.0);
        let kernel = ::angzarr_embedded::next::Kernel::new(&registry, &log)
            .on_retryable(capped())
            .wait_with(&waits);
        let sampled = Sampled {
            now: at(9),
            command_id: Uuid::from_bytes([0x79; 16]),
        };
        let failed = ready(kernel.handle_any(
            &store,
            send("picky", 2, any(Note {}), None),
            &sampled,
            &mut (),
        ));
        assert!(failed.is_err());
        assert_eq!(waits.micros(), [10_000, 30_000, 50_000]);
        let lines = log.lines();
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert!(
            lines[0].contains("picky: PICKY_BUSY retry_count=3"),
            "its own key, as last answered: {lines:?}"
        );

        let log = MemoryDeadLetters::new();
        let kernel = ::angzarr_embedded::next::Kernel::new(&registry, &log).on_retryable(
            ::angzarr_embedded::next::OnRetryable {
                max_retries: 0,
                ..capped()
            },
        );
        let resumed = ready(kernel.resume(&store, &sampled, &mut ())).expect("nothing left owed");
        assert!(
            resumed.steps().is_empty(),
            "the position moved past the trigger"
        );
    }

    /// Under `OnPolicyBug::Stay`, a refusal fact its source fails on with a
    /// bug is still dead-lettered, but the policy's position stays, so a
    /// resume runs the step again.
    #[test]
    fn under_stay_a_refusal_fact_its_source_fails_on_holds_the_position() {
        use ::angzarr_embedded::next::{Kernel, OnPolicyBug};
        let (registry, store) = picky();
        let log = MemoryDeadLetters::new();
        let kernel = Kernel::new(&registry, &log).on_policy_bug(OnPolicyBug::Stay);
        let sampled = Sampled {
            now: at(9),
            command_id: Uuid::from_bytes([0x79; 16]),
        };
        let failed = ready(kernel.handle_any(
            &store,
            send("picky", 2, any(Note {}), None),
            &sampled,
            &mut (),
        ))
        .err()
        .expect("the refusal fact's step fails");
        assert_the_fact_step_failed(&failed);
        assert_eq!(log.lines().len(), 1, "{:?}", log.lines());
        let resumed = ready(kernel.resume(&store, &sampled, &mut ()))
            .err()
            .expect("the position stayed, so the step runs again");
        assert_the_fact_step_failed(&resumed);
        assert_eq!(
            log.lines().len(),
            2,
            "dead-lettered again: {:?}",
            log.lines()
        );
    }

    /// A domain's input goes through the router too: a command type the
    /// wallet has no handler for is the router's `ROUTER_REASON_UNKNOWN_COMMAND_TYPE`.
    #[test]
    fn a_domains_unknown_command_is_the_routers_unknown_command_type() {
        let registry = registry();
        let store = Mem::new();
        let log = MemoryDeadLetters::new();
        let Err(failed) = handle_in::<Wallet>(
            &store,
            &registry,
            &log,
            1,
            send("wallet", 1, any(SwitchOn {}), None),
        ) else {
            panic!("a command the wallet does not handle is refused");
        };
        assert_eq!(
            router_reason(&failed),
            Some("ROUTER_REASON_UNKNOWN_COMMAND_TYPE"),
            "{failed}"
        );
        assert_eq!(pages(&store, wallet().cover()), None);
        // The router's own error on malformed input is a bug (INTERNAL;
        // the owner's ruling, 2026-10-29): alerted and dead-lettered under
        // the router's key, with its args, never a refusal (decision
        // 14.48).
        assert_eq!(failed.recovery(), Recovery::Bug);
        let lines = log.lines();
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert!(
            lines[0].contains("wallet: ROUTER_REASON_UNKNOWN_COMMAND_TYPE "),
            "{lines:?}"
        );
        assert!(
            lines[0].contains("type_url=/toy.lamp.cmd.v1.SwitchOn"),
            "{lines:?}"
        );
    }
}

// ------------------------------------------------------- the projectors
//
// ONE projector instance per process, registered with its ports, run
// AFTER COMMIT and awaited on the caller's runtime before the call
// returns, so a failure reaches the caller as an error; the events stay
// committed. Several projectors on one domain each receive its events,
// and one's failure does not stop the others.

/// The projector's port: the balances it wrote, and whether to fail.
#[derive(Clone, Default)]
pub struct BalancePort {
    lines: std::sync::Arc<std::sync::Mutex<Vec<(u32, i64)>>>,
    fail: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

/// Writes each deposit, numbered by how many THIS instance has written:
/// one instance across streams numbers them on.
pub struct Balances {
    port: BalancePort,
    written: u32,
}

#[projector(name = "balances", sources = [Wallet])]
impl Balances {
    #[projects(Deposited)]
    async fn deposited(
        &mut self,
        event: &Deposited,
        _ctx: &ProjectionContext,
    ) -> Result<(), Report> {
        if self.port.fail.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(Report::of(&Refused::NotOpen));
        }
        self.written += 1;
        self.port
            .lines
            .lock()
            .unwrap()
            .push((self.written, event.cents));
        Ok(())
    }
}

/// A port recording the events a projector saw, by type name, and
/// whether to fail.
#[derive(Clone, Default)]
pub struct TrailPort {
    seen: std::sync::Arc<std::sync::Mutex<Vec<&'static str>>>,
    fail: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl TrailPort {
    fn see(&self, name: &'static str) -> Result<(), Report> {
        if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(Report::of(&Refused::NotOpen));
        }
        self.seen.lock().unwrap().push(name);
        Ok(())
    }
}

/// A second projector on the wallet, marking two of its events.
pub struct Audit {
    port: TrailPort,
}

#[projector(name = "audit", sources = [Wallet])]
impl Audit {
    #[projects(Opened)]
    async fn opened(&mut self, _event: &Opened, _ctx: &ProjectionContext) -> Result<(), Report> {
        self.port.see("Opened")
    }

    #[projects(Deposited)]
    async fn deposited(
        &mut self,
        _event: &Deposited,
        _ctx: &ProjectionContext,
    ) -> Result<(), Report> {
        self.port.see("Deposited")
    }
}

/// A third, marking the same events.
pub struct Trail {
    port: TrailPort,
}

#[projector(name = "trail", sources = [Wallet])]
impl Trail {
    #[projects(Opened)]
    async fn opened(&mut self, _event: &Opened, _ctx: &ProjectionContext) -> Result<(), Report> {
        self.port.see("Opened")
    }

    #[projects(Deposited)]
    async fn deposited(
        &mut self,
        _event: &Deposited,
        _ctx: &ProjectionContext,
    ) -> Result<(), Report> {
        self.port.see("Deposited")
    }
}

/// ONE projector of TWO source domains: the wallet, named by its type, and
/// the lamp, by its name, as a domain with no Rust type here (a child's)
/// is named.
pub struct Lights {
    port: TrailPort,
}

#[projector(name = "lights", sources = [Wallet, "lamp"])]
impl Lights {
    #[projects(Opened)]
    async fn opened(&mut self, _event: &Opened, ctx: &ProjectionContext) -> Result<(), Report> {
        assert_eq!(ctx.stream().domain, "wallet");
        self.port.see("Opened")
    }

    #[projects(SwitchedOn)]
    async fn switched_on(
        &mut self,
        _event: &SwitchedOn,
        ctx: &ProjectionContext,
    ) -> Result<(), Report> {
        assert_eq!(ctx.stream().domain, "lamp");
        self.port.see("SwitchedOn")
    }
}

/// Poll `f` to completion with a waker that does nothing: enough for the
/// in-memory store, which is ready at once.
fn ready<F: std::future::Future>(f: F) -> F::Output {
    use std::task::{Context as Cx, Poll, RawWaker, RawWakerVTable, Waker};
    fn noop(_: *const ()) {}
    fn clone(p: *const ()) -> RawWaker {
        RawWaker::new(p, &VTABLE)
    }
    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, noop, noop, noop);
    // SAFETY: every vtable function ignores the data pointer.
    let waker = unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) };
    let mut f = std::pin::pin!(f);
    match f.as_mut().poll(&mut Cx::from_waker(&waker)) {
        Poll::Ready(out) => out,
        Poll::Pending => panic!("not ready at once"),
    }
}

/// [`ready`], as a method: `stream.execute(..).ready()`.
trait Ready: std::future::Future + Sized {
    fn ready(self) -> Self::Output {
        ready(self)
    }
}

impl<F: std::future::Future> Ready for F {}

/// Handle `payload` as command `n` to the wallet at `root`, on the async
/// path, awaited.
fn handle_async(
    store: &Mem,
    registry: &Registry,
    n: u8,
    payload: Any,
    root: Uuid,
) -> Result<Handled, Failure<Rejected>> {
    let sampled = Sampled {
        now: at(i64::from(n)),
        command_id: Uuid::from_bytes([n; 16]),
    };
    let mut book = send("wallet", n, payload, None);
    book.cover.as_mut().unwrap().root = Some(root);
    let log = MemoryDeadLetters::new();
    ready(
        ::angzarr_embedded::next::Kernel::new(registry, &log).handle::<Wallet, _, _>(
            store,
            book,
            &sampled,
            &mut (),
        ),
    )
}

mod projector {
    use super::*;

    fn projected(port: &BalancePort) -> Registry {
        let mut registry = registry();
        registry.projector(Balances {
            port: port.clone(),
            written: 0,
        });
        registry
    }

    /// Run after the commit and awaited: the read model holds the deposit
    /// once the call returns.
    #[test]
    fn it_runs_after_commit_and_is_awaited_before_the_call_returns() {
        let port = BalancePort::default();
        let registry = projected(&port);
        let store = Mem::new();
        handle_async(&store, &registry, 1, any(Open { bonus: 5 }), root()).unwrap();
        assert_eq!(*port.lines.lock().unwrap(), [(1, 5)]);
    }

    /// One instance per process: two streams' deposits reach the same
    /// instance, which numbers them on.
    #[test]
    fn one_instance_serves_every_stream() {
        let port = BalancePort::default();
        let registry = projected(&port);
        let store = Mem::new();
        handle_async(&store, &registry, 1, any(Open { bonus: 5 }), root()).unwrap();
        let other = Uuid::from_bytes([0x0b; 16]);
        handle_async(&store, &registry, 2, any(Open { bonus: 7 }), other).unwrap();
        assert_eq!(*port.lines.lock().unwrap(), [(1, 5), (2, 7)]);
    }

    /// An event it does not mark is skipped.
    #[test]
    fn an_unmarked_event_is_skipped() {
        let port = BalancePort::default();
        let registry = projected(&port);
        let store = Mem::new();
        handle_async(&store, &registry, 1, any(Open { bonus: 5 }), root()).unwrap();
        handle_async(&store, &registry, 2, any(Withdraw { cents: 5 }), root()).unwrap();
        assert_eq!(*port.lines.lock().unwrap(), [(1, 5)]);
    }

    /// A failure is the caller's ERROR, raised after the commit: a
    /// `Failure::Projection` naming the projector and carrying what the
    /// chain committed, and those events stay committed; its recovery is
    /// a retry.
    #[test]
    fn a_failure_is_the_callers_error_and_the_events_stay_committed() {
        let port = BalancePort::default();
        port.fail.store(true, std::sync::atomic::Ordering::SeqCst);
        let registry = projected(&port);
        let store = Mem::new();
        let failed = handle_async(&store, &registry, 1, any(Open { bonus: 5 }), root());
        let Err(Failure::Projection {
            committed,
            failures,
        }) = failed
        else {
            panic!("a failing projection is the caller's Failure::Projection");
        };
        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].0, "balances");
        assert_eq!(failures[0].1.code(), "toy-wallet-not-open");
        assert_eq!(
            committed
                .iter()
                .map(|book| book.pages.len())
                .collect::<Vec<_>>(),
            [2],
            "the wallet's book: Opened and Deposited"
        );
        let stored = pages(&store, wallet().cover()).expect("the stream");
        assert_eq!(stored.len(), 2, "and they stay committed");
        assert_eq!(
            Failure::<Rejection<Refused>>::Projection {
                committed,
                failures,
            }
            .recovery(),
            Recovery::Retry
        );
    }

    /// Fan-out: every projector registered on one domain receives every
    /// committed event of that domain it marks, each in its own instance.
    #[test]
    fn every_projector_on_a_domain_receives_its_events() {
        let port = BalancePort::default();
        let (audit, trail) = (TrailPort::default(), TrailPort::default());
        let mut registry = projected(&port);
        registry
            .projector(Audit {
                port: audit.clone(),
            })
            .projector(Trail {
                port: trail.clone(),
            });
        let store = Mem::new();
        handle_async(&store, &registry, 1, any(Open { bonus: 5 }), root()).unwrap();
        assert_eq!(*port.lines.lock().unwrap(), [(1, 5)]);
        assert_eq!(*audit.seen.lock().unwrap(), ["Opened", "Deposited"]);
        assert_eq!(*trail.seen.lock().unwrap(), ["Opened", "Deposited"]);
    }

    /// One projector's failure does not stop the others: each still
    /// receives its events, and every failure is reported in the one
    /// `Failure::Projection`, in registration order.
    #[test]
    fn a_projectors_failure_does_not_stop_the_others() {
        let port = BalancePort::default();
        port.fail.store(true, std::sync::atomic::Ordering::SeqCst);
        let (audit, trail) = (TrailPort::default(), TrailPort::default());
        audit.fail.store(true, std::sync::atomic::Ordering::SeqCst);
        let mut registry = projected(&port);
        registry
            .projector(Audit {
                port: audit.clone(),
            })
            .projector(Trail {
                port: trail.clone(),
            });
        let store = Mem::new();
        let failed = handle_async(&store, &registry, 1, any(Open { bonus: 5 }), root());
        let Err(Failure::Projection { failures, .. }) = failed else {
            panic!("the failures are the caller's Failure::Projection");
        };
        assert_eq!(
            failures
                .iter()
                .map(|(name, _)| name.as_str())
                .collect::<Vec<_>>(),
            ["balances", "audit"]
        );
        assert_eq!(*trail.seen.lock().unwrap(), ["Opened", "Deposited"]);
    }

    /// One projector of several source domains: its sources, in marker
    /// order, by a domain type's name or a name as written; every
    /// committed event of each it marks reaches its one instance.
    #[test]
    fn one_projector_projects_several_source_domains() {
        assert_eq!(
            <Lights as ::angzarr_embedded::next::markup::AsyncProjector>::SOURCES,
            ["wallet", "lamp"]
        );
        let trail = TrailPort::default();
        let mut registry = registry();
        registry.projector(Lights {
            port: trail.clone(),
        });
        let store = Mem::new();
        handle_async(&store, &registry, 1, any(Open { bonus: 5 }), root()).unwrap();
        let log = MemoryDeadLetters::new();
        let sampled = Sampled {
            now: at(2),
            command_id: Uuid::from_bytes([2; 16]),
        };
        ready(
            ::angzarr_embedded::next::Kernel::new(&registry, &log).handle::<Lamp, _, _>(
                &store,
                send("lamp", 2, any(SwitchOn {}), None),
                &sampled,
                &mut (),
            ),
        )
        .unwrap();
        assert_eq!(*trail.seen.lock().unwrap(), ["Opened", "SwitchedOn"]);
    }

    /// A projector written by hand as the router's `ProjectorHandler`:
    /// the books of each stream it is registered for, by domain.
    struct Books(std::sync::Arc<std::sync::Mutex<Vec<String>>>);

    impl ::angzarr_embedded::next::angzarr_router::router::ProjectorHandler for Books {
        fn name(&self) -> &str {
            "books"
        }

        async fn dispatch(
            &self,
            events: &EventBook,
        ) -> Result<
            ::angzarr_embedded::next::v1::Projection,
            ::angzarr_embedded::next::angzarr_router::error::CodedError,
        > {
            let domain = events.cover.clone().unwrap_or_default().domain;
            self.0.lock().unwrap().push(domain);
            Ok(::angzarr_embedded::next::v1::Projection::default())
        }
    }

    /// A hand-written projector registered for several source domains
    /// (`Registry::projector_handler`) is handed each committed book of
    /// every one of them, and of no other.
    #[test]
    fn a_hand_written_projector_projects_the_domains_it_is_registered_for() {
        let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut registry = registry();
        registry.projector_handler(&["wallet", "lamp"], Books(seen.clone()));
        let store = Mem::new();
        handle_async(&store, &registry, 1, any(Open { bonus: 5 }), root()).unwrap();
        let log = MemoryDeadLetters::new();
        let at_ = |n: u8| Sampled {
            now: at(i64::from(n)),
            command_id: Uuid::from_bytes([n; 16]),
        };
        let kernel = ::angzarr_embedded::next::Kernel::new(&registry, &log);
        ready(kernel.handle::<Lamp, _, _>(
            &store,
            send("lamp", 2, any(SwitchOn {}), None),
            &at_(2),
            &mut (),
        ))
        .unwrap();
        ready(kernel.handle::<Meter, _, _>(
            &store,
            send("meter", 3, any(Calibrate {}), None),
            &at_(3),
            &mut (),
        ))
        .unwrap();
        assert_eq!(*seen.lock().unwrap(), ["wallet", "lamp"]);
    }

    #[test]
    #[should_panic(expected = "balances")]
    fn registering_a_projector_twice_panics() {
        let port = BalancePort::default();
        let mut registry = projected(&port);
        registry.projector(Balances { port, written: 0 });
    }
}
