//! A child's descriptor, validated by the host (decision 14.42, C-0687):
//! angzarr-cli generates the `ComponentDescriptor` at build time and the
//! child answers `Describe` with it unchanged; the host, as its
//! coordinator, refuses one that fails a rule, as a readiness failure,
//! naming the component and the reason key (a `CoordinatorReason`):
//! [`Host::start`](crate::Host::start) refuses such a child as
//! [`HostError::Descriptor`](crate::HostError::Descriptor), after its kind
//! is checked against its `kind`, and on every restart.

use std::fmt;

use angzarr_diagnostic::{Arg, Diagnostic};
use angzarr_embedded::next::reasons::CoordinatorReason;
use angzarr_router::pb::{ComponentDescriptor, ComponentKind};

/// Why a descriptor is refused: one variant per reason key, each naming
/// the component, in the order the rules are checked.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DescriptorRefused {
    /// `COORDINATOR_REASON_MISSING_RULES_VERSION`: an empty rules version.
    MissingRulesVersion { component: String },
    /// `COORDINATOR_REASON_STARTING_TYPES_ON_NON_PM`: starting types on a kind that is not a
    /// process manager.
    StartingTypesOnNonPm { component: String, kind: String },
    /// `COORDINATOR_REASON_MISSING_STATE_TYPE`: an aggregate or process manager with no state.
    MissingStateType { component: String, kind: String },
    /// `COORDINATOR_REASON_STATE_ON_STATELESS_KIND`: a policy or projector with a state.
    StateOnStatelessKind { component: String, kind: String },
    /// `COORDINATOR_REASON_POLICY_INPUT_COUNT`: a policy with other than exactly one input.
    PolicyInputCount { component: String, input_count: u64 },
    /// `COORDINATOR_REASON_POLICY_NO_OUTPUT_DOMAIN`: a policy with no output domain.
    PolicyNoOutputDomain { component: String },
    /// `COORDINATOR_REASON_OUTPUT_DOMAIN_ON_NON_POLICY`: an output domain on any other kind.
    OutputDomainOnNonPolicy { component: String, kind: String },
}

impl DescriptorRefused {
    /// The reason key: its `CoordinatorReason` value's full name, as it
    /// travels.
    pub fn reason(&self) -> &'static str {
        match self {
            DescriptorRefused::MissingRulesVersion { .. } => {
                CoordinatorReason::MissingRulesVersion.as_str_name()
            }
            DescriptorRefused::StartingTypesOnNonPm { .. } => {
                CoordinatorReason::StartingTypesOnNonPm.as_str_name()
            }
            DescriptorRefused::MissingStateType { .. } => {
                CoordinatorReason::MissingStateType.as_str_name()
            }
            DescriptorRefused::StateOnStatelessKind { .. } => {
                CoordinatorReason::StateOnStatelessKind.as_str_name()
            }
            DescriptorRefused::PolicyInputCount { .. } => {
                CoordinatorReason::PolicyInputCount.as_str_name()
            }
            DescriptorRefused::PolicyNoOutputDomain { .. } => {
                CoordinatorReason::PolicyNoOutputDomain.as_str_name()
            }
            DescriptorRefused::OutputDomainOnNonPolicy { .. } => {
                CoordinatorReason::OutputDomainOnNonPolicy.as_str_name()
            }
        }
    }
}

/// `descriptor`, held to 14.42's rules, in order: a rules version; starting
/// types only on a process manager; a state on an aggregate and a process
/// manager, none on a policy or a projector; a policy's one input and its
/// output domain, and no output domain on any other kind.
pub fn validate(descriptor: &ComponentDescriptor) -> Result<(), DescriptorRefused> {
    let component = || descriptor.name.clone();
    let kind = descriptor.kind();
    let kind_name = || kind.as_str_name().to_string();
    if descriptor.rules_version.is_empty() {
        return Err(DescriptorRefused::MissingRulesVersion {
            component: component(),
        });
    }
    if kind != ComponentKind::ProcessManager && !descriptor.starting_types.is_empty() {
        return Err(DescriptorRefused::StartingTypesOnNonPm {
            component: component(),
            kind: kind_name(),
        });
    }
    let stateful = matches!(
        kind,
        ComponentKind::Aggregate | ComponentKind::ProcessManager
    );
    let stateless = matches!(kind, ComponentKind::Policy | ComponentKind::Projector);
    if stateful && descriptor.state.is_none() {
        return Err(DescriptorRefused::MissingStateType {
            component: component(),
            kind: kind_name(),
        });
    }
    if stateless && descriptor.state.is_some() {
        return Err(DescriptorRefused::StateOnStatelessKind {
            component: component(),
            kind: kind_name(),
        });
    }
    if kind == ComponentKind::Policy {
        if descriptor.inputs.len() != 1 {
            return Err(DescriptorRefused::PolicyInputCount {
                component: component(),
                input_count: descriptor.inputs.len() as u64,
            });
        }
        if descriptor.output_domain.is_empty() {
            return Err(DescriptorRefused::PolicyNoOutputDomain {
                component: component(),
            });
        }
    } else if !descriptor.output_domain.is_empty() {
        return Err(DescriptorRefused::OutputDomainOnNonPolicy {
            component: component(),
            kind: kind_name(),
        });
    }
    Ok(())
}

impl fmt::Display for DescriptorRefused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl Diagnostic for DescriptorRefused {
    fn code(&self) -> &str {
        match self {
            DescriptorRefused::MissingRulesVersion { .. } => {
                "angzarr-embedded-host-descriptor-missing-rules-version"
            }
            DescriptorRefused::StartingTypesOnNonPm { .. } => {
                "angzarr-embedded-host-descriptor-starting-types-on-non-pm"
            }
            DescriptorRefused::MissingStateType { .. } => {
                "angzarr-embedded-host-descriptor-missing-state-type"
            }
            DescriptorRefused::StateOnStatelessKind { .. } => {
                "angzarr-embedded-host-descriptor-state-on-stateless-kind"
            }
            DescriptorRefused::PolicyInputCount { .. } => {
                "angzarr-embedded-host-descriptor-policy-input-count"
            }
            DescriptorRefused::PolicyNoOutputDomain { .. } => {
                "angzarr-embedded-host-descriptor-policy-no-output-domain"
            }
            DescriptorRefused::OutputDomainOnNonPolicy { .. } => {
                "angzarr-embedded-host-descriptor-output-domain-on-non-policy"
            }
        }
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        let component = |c: &str| ("component", Arg::from(c));
        let kind = |k: &str| ("component_kind", Arg::from(k));
        match self {
            DescriptorRefused::MissingRulesVersion { component: c }
            | DescriptorRefused::PolicyNoOutputDomain { component: c } => vec![component(c)],
            DescriptorRefused::StartingTypesOnNonPm {
                component: c,
                kind: k,
            }
            | DescriptorRefused::MissingStateType {
                component: c,
                kind: k,
            }
            | DescriptorRefused::StateOnStatelessKind {
                component: c,
                kind: k,
            }
            | DescriptorRefused::OutputDomainOnNonPolicy {
                component: c,
                kind: k,
            } => {
                vec![component(c), kind(k)]
            }
            DescriptorRefused::PolicyInputCount {
                component: c,
                input_count,
            } => vec![component(c), ("input_count", (*input_count).into())],
        }
    }
}

impl std::error::Error for DescriptorRefused {}

/// One value of every variant, for the catalog parity test.
pub(crate) fn diagnostic_samples() -> Vec<Box<dyn Diagnostic>> {
    let c = || "order".to_string();
    let k = || ComponentKind::Aggregate.as_str_name().to_string();
    vec![
        Box::new(DescriptorRefused::MissingRulesVersion { component: c() }),
        Box::new(DescriptorRefused::StartingTypesOnNonPm {
            component: c(),
            kind: k(),
        }),
        Box::new(DescriptorRefused::MissingStateType {
            component: c(),
            kind: k(),
        }),
        Box::new(DescriptorRefused::StateOnStatelessKind {
            component: c(),
            kind: ComponentKind::Projector.as_str_name().to_string(),
        }),
        Box::new(DescriptorRefused::PolicyInputCount {
            component: c(),
            input_count: 2,
        }),
        Box::new(DescriptorRefused::PolicyNoOutputDomain { component: c() }),
        Box::new(DescriptorRefused::OutputDomainOnNonPolicy {
            component: c(),
            kind: ComponentKind::ProcessManager.as_str_name().to_string(),
        }),
    ]
}
