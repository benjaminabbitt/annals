//! The registry: angzarr-embedded's mirror of `ComponentDescriptor.state`, looked up
//! by `cover.domain`, and the conflict-resolution defaults. These run.

use crate::v1::{ConflictResolution, Snapshot};
use crate::{pack, Misconfigured, Registry, Upcaster};

use crate::toys::{account, registry, shelf, Account};

#[test]
fn registration_fills_the_state_descriptor_from_the_generated_impl() {
    let registry = registry();
    let shelf = registry.state("shelf").unwrap();
    assert_eq!(shelf.type_url, "/toy.shelf.v1.ShelfState");
    assert!(registry.supports("shelf", ConflictResolution::Commutative));
    assert!(registry.supports("shelf", ConflictResolution::Strict));
    assert!(registry.supports("account", ConflictResolution::AggregateHandles));
    assert!(
        !registry.supports("account", ConflictResolution::Commutative),
        "no conflict units"
    );
    assert!(!registry.supports("nowhere", ConflictResolution::Strict));
    assert_eq!(registry.rules_version("account").as_deref(), Some("v2"));
    assert_eq!(
        registry.domains().collect::<Vec<_>>(),
        ["account", "ledger", "shelf"]
    );
}

#[test]
fn commutative_is_the_default_where_the_state_declares_units_else_strict() {
    let registry = registry();
    let set = "/toy.shelf.cmd.v1.Set";
    let deposit = "/toy.account.cmd.v1.Deposit";
    assert_eq!(
        registry.resolve("shelf", set, None),
        Some(ConflictResolution::Commutative),
        "the built-in default where the state supports it"
    );
    assert_eq!(
        registry.resolve("account", deposit, None),
        Some(ConflictResolution::Strict),
        "and STRICT where it does not"
    );
    assert_eq!(
        registry.resolve("shelf", "/toy.shelf.cmd.v1.Note", None),
        Some(ConflictResolution::Strict),
        "a command type's configuration is above the default"
    );
    assert_eq!(
        registry.resolve(
            "shelf",
            "type.googleapis.com/toy.shelf.cmd.v1.Note",
            Some(ConflictResolution::AggregateHandles)
        ),
        Some(ConflictResolution::AggregateHandles),
        "and the book's request above that"
    );
    assert_eq!(registry.resolve("nowhere", set, None), None);
}

#[test]
fn misconfiguration_is_refused_at_registration() {
    let mut registry = registry();
    assert_eq!(
        registry.register::<Account>(
            Upcaster::new("u", "account"),
            crate::toys::handlers::<crate::toys::Account>()
        ),
        Err(Misconfigured::AlreadyRegistered {
            domain: "account".to_string()
        })
    );

    let mut fresh = Registry::new();
    assert_eq!(
        fresh.register::<Account>(
            Upcaster::new("u", "ledger"),
            crate::toys::handlers::<crate::toys::Account>()
        ),
        Err(Misconfigured::UpcasterForAnotherDomain {
            domain: "account".to_string(),
            upcaster: "ledger".to_string()
        })
    );
    /// An account built without a version.
    struct Unversioned;
    impl crate::Domain for Unversioned {
        type State = account::AccountState;
        type Command = std::convert::Infallible;
        type Fact = std::convert::Infallible;
        type Event = crate::toys::AccountEvent;
        type Rejection = std::convert::Infallible;
        const RULES_VERSION: &'static str = "";
        const NAME: &'static str = "unversioned";
        fn apply(_: &mut account::AccountState, _: &Self::Event) {}
    }
    assert_eq!(
        fresh.register::<Unversioned>(Upcaster::new("u", "unversioned"), |t| t),
        Err(Misconfigured::EmptyRulesVersion {
            domain: "unversioned".to_string()
        })
    );
    assert!(fresh.domains().next().is_none(), "nothing half-registered");
}

#[test]
fn a_configured_resolution_the_state_lacks_is_refused() {
    use crate::toys::account::AccountState;
    use crate::Domain;

    /// An account configured COMMUTATIVE although its state has no units.
    struct Eager;
    impl Domain for Eager {
        type State = AccountState;
        type Command = std::convert::Infallible;
        type Fact = std::convert::Infallible;
        type Event = crate::toys::AccountEvent;
        type Rejection = std::convert::Infallible;
        const RULES_VERSION: &'static str = "v1";
        const NAME: &'static str = "eager";
        const CONFLICT_RESOLUTION: Option<ConflictResolution> =
            Some(ConflictResolution::Commutative);
        fn apply(_: &mut AccountState, _: &Self::Event) {}
    }

    assert_eq!(
        Registry::new().register::<Eager>(Upcaster::new("u", "eager"), |t| t),
        Err(Misconfigured::UnsupportedResolution {
            domain: "eager".to_string(),
            command_type: None,
            resolution: ConflictResolution::Commutative
        })
    );
}

#[test]
fn a_snapshot_is_usable_only_under_a_compatible_version_and_the_state_type() {
    let registry = registry();
    let state = pack(&account::AccountState::default());
    let snapshot = |rules: &str, state: &crate::Any| Snapshot {
        sequence: 3,
        state: Some(state.clone()),
        rules_version: rules.to_string(),
    };
    assert!(registry.snapshot_usable("account", &snapshot("v2", &state)));
    assert!(
        !registry.snapshot_usable("account", &snapshot("v1", &state)),
        "another build's, not SemVer: not offered"
    );
    assert!(
        !registry.snapshot_usable(
            "account",
            &snapshot("v2", &pack(&shelf::ShelfState::default()))
        ),
        "another state type: discarded"
    );
    assert!(!registry.snapshot_usable("nowhere", &snapshot("v2", &state)));
}

#[test]
fn a_state_descriptor_without_the_implicit_base_is_refused() {
    use crate::v1::{ConflictResolution, StateDescriptor, StateOptions};

    /// Lists STRICT only: not the base every generated descriptor has.
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct StrictOnly {}

    impl prost::Name for StrictOnly {
        const NAME: &'static str = "StrictOnly";
        const PACKAGE: &'static str = "toy.bad.v1";
    }

    impl crate::State for StrictOnly {
        type Snapshot = Self;

        fn to_snapshot(&self) -> Self {
            self.clone()
        }

        fn from_snapshot(snapshot: Self) -> Result<Self, angzarr_diagnostic::Report> {
            Ok(snapshot)
        }

        fn descriptor() -> StateDescriptor {
            StateDescriptor {
                type_url: "/toy.bad.v1.StrictOnly".to_string(),
                options: Some(StateOptions {
                    conflict_resolutions: vec![ConflictResolution::Strict as i32],
                }),
            }
        }
    }

    struct Bad;

    impl crate::Domain for Bad {
        type State = StrictOnly;
        type Command = std::convert::Infallible;
        type Fact = std::convert::Infallible;
        type Event = std::convert::Infallible;
        type Rejection = std::convert::Infallible;
        const RULES_VERSION: &'static str = "v1";

        const NAME: &'static str = "bad";

        fn apply(_: &mut StrictOnly, event: &std::convert::Infallible) {
            match *event {}
        }
    }

    let mut registry = crate::Registry::new();
    assert_eq!(
        registry.register::<Bad>(crate::Upcaster::new("u", "bad"), |t| t),
        Err(crate::Misconfigured::BadStateDescriptor {
            domain: "bad".to_string()
        })
    );
}

#[test]
fn a_state_descriptor_naming_another_type_than_its_snapshot_is_refused() {
    use crate::v1::{ConflictResolution, StateDescriptor, StateOptions};

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Snap {}

    impl prost::Name for Snap {
        const NAME: &'static str = "Snap";
        const PACKAGE: &'static str = "toy.bad.v1";
    }

    /// Its descriptor names a type its snapshots are not: a snapshot it
    /// wrote would never be found usable.
    #[derive(Clone, Default)]
    pub struct Misnamed;

    impl crate::State for Misnamed {
        type Snapshot = Snap;

        fn to_snapshot(&self) -> Snap {
            Snap {}
        }

        fn from_snapshot(_: Snap) -> Result<Self, angzarr_diagnostic::Report> {
            Ok(Misnamed)
        }

        fn descriptor() -> StateDescriptor {
            StateDescriptor {
                type_url: "/toy.bad.v1.Elsewhere".to_string(),
                options: Some(StateOptions {
                    conflict_resolutions: vec![
                        ConflictResolution::Strict as i32,
                        ConflictResolution::AggregateHandles as i32,
                    ],
                }),
            }
        }
    }

    struct Bad;

    impl crate::Domain for Bad {
        type State = Misnamed;
        type Command = std::convert::Infallible;
        type Fact = std::convert::Infallible;
        type Event = std::convert::Infallible;
        type Rejection = std::convert::Infallible;
        const RULES_VERSION: &'static str = "v1";

        const NAME: &'static str = "bad";

        fn apply(_: &mut Misnamed, event: &std::convert::Infallible) {
            match *event {}
        }
    }

    let mut registry = crate::Registry::new();
    assert_eq!(
        registry.register::<Bad>(crate::Upcaster::new("u", "bad"), |t| t),
        Err(crate::Misconfigured::BadStateDescriptor {
            domain: "bad".to_string()
        })
    );
}

/// A hand-written handler of `kind`, registered by its descriptor.
struct Hello {
    kind: angzarr_router::pb::ComponentKind,
}

impl angzarr_router::router::CommandHandler for Hello {
    fn name(&self) -> &str {
        "hello"
    }

    fn kind(&self) -> angzarr_router::pb::ComponentKind {
        self.kind
    }

    fn command_types(&self) -> Vec<String> {
        Vec::new()
    }

    async fn dispatch(
        &self,
        _: &angzarr_router::pb::ContextualCommand,
    ) -> Result<angzarr_router::pb::CommandHandlerResponse, angzarr_router::error::CodedError> {
        Ok(Default::default())
    }
}

fn hello(state: Option<crate::v1::StateDescriptor>) -> angzarr_router::pb::ComponentDescriptor {
    angzarr_router::pb::ComponentDescriptor {
        name: "hello".to_string(),
        rules_version: "h1".to_string(),
        state,
        ..Default::default()
    }
}

fn base_state() -> crate::v1::StateDescriptor {
    use crate::v1::{ConflictResolution, StateDescriptor, StateOptions};
    StateDescriptor {
        type_url: "/toy.hello.v1.State".to_string(),
        options: Some(StateOptions {
            conflict_resolutions: vec![
                ConflictResolution::Strict as i32,
                ConflictResolution::AggregateHandles as i32,
            ],
        }),
    }
}

/// A handler registers on what its descriptor says: refused without a
/// rules version or a state type, a process manager's kind kept.
#[test]
fn a_command_handler_registers_by_its_descriptor() {
    use angzarr_router::pb::ComponentKind;
    let refused = |descriptor| {
        crate::Registry::new()
            .command_handler(
                "hello",
                Hello {
                    kind: ComponentKind::Aggregate,
                },
                descriptor,
            )
            .err()
    };
    let unversioned = angzarr_router::pb::ComponentDescriptor {
        rules_version: String::new(),
        ..hello(Some(base_state()))
    };
    assert_eq!(
        refused(unversioned),
        Some(Misconfigured::EmptyRulesVersion {
            domain: "hello".to_string()
        })
    );
    let untyped = crate::v1::StateDescriptor {
        type_url: String::new(),
        ..base_state()
    };
    assert_eq!(
        refused(hello(Some(untyped))),
        Some(Misconfigured::BadStateDescriptor {
            domain: "hello".to_string()
        })
    );
    assert_eq!(
        refused(hello(None)),
        Some(Misconfigured::BadStateDescriptor {
            domain: "hello".to_string()
        })
    );

    let mut registry = crate::Registry::new();
    registry
        .command_handler(
            "hello",
            Hello {
                kind: ComponentKind::ProcessManager,
            },
            hello(Some(base_state())),
        )
        .unwrap();
    assert!(registry.is_process_manager("hello"));
    assert_eq!(registry.kind("hello"), ComponentKind::ProcessManager);
    assert_eq!(registry.kind("nowhere"), ComponentKind::Unspecified);
    assert_eq!(registry.rules_version("hello").as_deref(), Some("h1"));
    let mut aggregate = crate::Registry::new();
    aggregate
        .command_handler(
            "hello",
            Hello {
                kind: ComponentKind::Aggregate,
            },
            hello(Some(base_state())),
        )
        .unwrap();
    assert!(!aggregate.is_process_manager("hello"));
    assert_eq!(aggregate.kind("hello"), ComponentKind::Aggregate);
}

/// C-0563: a domain registered by its descriptor is described again
/// through its [`Description`](crate::Description): the new
/// descriptor replaces the old whole (its rules version, its state, a
/// process manager's starting types), and one the registry refuses
/// leaves the old in place.
#[test]
fn a_described_domain_is_replaced_whole_by_its_new_descriptor() {
    use angzarr_router::pb::ComponentKind;
    let mut registry = crate::Registry::new();
    let description = registry
        .described_handler(
            "hello",
            Hello {
                kind: ComponentKind::ProcessManager,
            },
            angzarr_router::pb::ComponentDescriptor {
                starting_types: vec!["toy.hello.v1.Start".to_string()],
                ..hello(Some(base_state()))
            },
        )
        .unwrap();
    assert_eq!(
        format!("{description:?}"),
        "Description { domain: \"hello\", .. }"
    );
    let commutative = crate::v1::StateDescriptor {
        type_url: "/toy.hello.v2.State".to_string(),
        options: Some(crate::v1::StateOptions {
            conflict_resolutions: vec![
                ConflictResolution::Commutative as i32,
                ConflictResolution::Strict as i32,
                ConflictResolution::AggregateHandles as i32,
            ],
        }),
    };
    description
        .replace(angzarr_router::pb::ComponentDescriptor {
            rules_version: "h2".to_string(),
            starting_types: vec!["toy.hello.v1.Begin".to_string()],
            ..hello(Some(commutative.clone()))
        })
        .unwrap();
    assert_eq!(registry.rules_version("hello").as_deref(), Some("h2"));
    assert_eq!(registry.state("hello"), Some(commutative));
    assert!(registry.supports("hello", ConflictResolution::Commutative));
    assert_eq!(
        registry.starting_types("hello"),
        Some(["toy.hello.v1.Begin".to_string()].into())
    );

    let refused = description.replace(angzarr_router::pb::ComponentDescriptor {
        rules_version: String::new(),
        ..hello(Some(base_state()))
    });
    assert_eq!(
        refused,
        Err(Misconfigured::EmptyRulesVersion {
            domain: "hello".to_string()
        })
    );
    assert_eq!(
        registry.rules_version("hello").as_deref(),
        Some("h2"),
        "the old kept"
    );
    assert!(registry
        .described_handler(
            "hello",
            Hello {
                kind: ComponentKind::Aggregate,
            },
            hello(Some(base_state())),
        )
        .is_err());
}

/// A domain's name as a component of an application is decision 14.50's
/// default: an aggregate's is its domain, a process manager's `pmg-` and
/// its domain; a domain not registered here has none.
#[test]
fn a_domain_is_named_as_a_component_by_its_kind() {
    use angzarr_router::pb::ComponentKind;
    let named = |kind| {
        let mut registry = crate::Registry::new();
        registry
            .command_handler("hello", Hello { kind }, hello(Some(base_state())))
            .unwrap();
        (
            registry.component_name("hello"),
            registry.component_name("nowhere"),
        )
    };
    assert_eq!(
        named(ComponentKind::Aggregate),
        (Some("hello".to_string()), None)
    );
    assert_eq!(
        named(ComponentKind::ProcessManager),
        (Some("pmg-hello".to_string()), None)
    );
}

/// A projector, as the router's `ProjectorHandler`, named `name`, that
/// projects nothing.
struct Named(&'static str);

impl angzarr_router::router::ProjectorHandler for Named {
    fn name(&self) -> &str {
        self.0
    }

    async fn dispatch(
        &self,
        _: &crate::v1::EventBook,
    ) -> Result<crate::v1::Projection, angzarr_router::error::CodedError> {
        Ok(crate::v1::Projection::default())
    }
}

/// The registry names its policies and its projectors, each in
/// registration order.
#[test]
fn the_registry_names_its_policies_and_projectors_in_order() {
    use angzarr_router::policy::PolicyDispatch;
    let mut registry = Registry::new();
    assert_eq!(registry.policy_names().count(), 0);
    assert_eq!(registry.projector_names().count(), 0);
    registry
        .policy_dispatch(PolicyDispatch::new("b-policy", "x", "y"))
        .policy_dispatch(PolicyDispatch::new("a-policy", "x", "y"))
        .projector_handler(&["x"], Named("b-view"))
        .projector_handler(&["x", "y"], Named("a-view"));
    assert_eq!(
        registry.policy_names().collect::<Vec<_>>(),
        ["b-policy", "a-policy"]
    );
    assert_eq!(
        registry.projector_names().collect::<Vec<_>>(),
        ["b-view", "a-view"]
    );
}

/// A projector projects at least one source domain: one naming none is
/// a composition-root bug, refused at startup by its name.
#[test]
#[should_panic(expected = "no-view")]
fn a_projector_with_no_source_panics() {
    Registry::new().projector_handler(&[], Named("no-view"));
}

/// types.proto's Snapshot region, the default rule: a snapshot is offered
/// when its version, one leading "v" or "V" stripped, has the registered
/// version's SemVer major and minor (C-0522, C-0523); a version that is
/// not SemVer, so stripped, only under exact equality (C-0524).
#[test]
fn a_snapshot_is_usable_under_the_registered_major_and_minor() {
    let mut registry = crate::Registry::new();
    for (domain, version) in [("hello", "1.4.2+build.7"), ("label", "release-7")] {
        registry
            .command_handler(
                domain,
                Hello {
                    kind: angzarr_router::pb::ComponentKind::Aggregate,
                },
                angzarr_router::pb::ComponentDescriptor {
                    rules_version: version.to_string(),
                    ..hello(Some(base_state()))
                },
            )
            .unwrap();
    }
    let snapshot = |rules: &str| Snapshot {
        sequence: 3,
        state: Some(crate::Any {
            type_url: "/toy.hello.v1.State".to_string(),
            value: Vec::new(),
        }),
        rules_version: rules.to_string(),
    };
    for usable in [
        "1.4.2+build.7",
        "1.4.0",
        "v1.4.9",
        "V1.4.2",
        "1.4.3-rc.1",
        "1.4.3-rc-1.x-y",
        "1.4.3+build-7.a-b",
        "1.4.3-0",
    ] {
        assert!(
            registry.snapshot_usable("hello", &snapshot(usable)),
            "{usable}"
        );
    }
    for unusable in [
        "1.5.0",
        "2.4.2",
        "0.4.2",
        "vv1.4.2",
        "1.4",
        "",
        "1.04.2",
        "x1.4.2",
        "1.4.2.0",
        // Not SemVer, so equal only to themselves: an empty or invalid
        // build or pre-release identifier, a numeric one with a leading 0.
        "1.4.2+",
        "1.4.2+build..7",
        "1.4.2+bu_ild",
        "1.4.2-",
        "1.4.2-rc_1",
        "1.4.2-01",
    ] {
        assert!(
            !registry.snapshot_usable("hello", &snapshot(unusable)),
            "{unusable}"
        );
    }
    assert!(registry.snapshot_usable("label", &snapshot("release-7")));
    assert!(!registry.snapshot_usable("label", &snapshot("release-8")));
    assert!(!registry.snapshot_usable("label", &snapshot("vrelease-7")));
}
