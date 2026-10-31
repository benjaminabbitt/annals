//! What the expansion refuses, and why, by its messages: the
//! `compile_fail` examples pin that each misuse fails to compile; these pin
//! that it fails for its own reason.

use proc_macro2::TokenStream as Tokens;
use quote::quote;

use super::{domain, flow, Flow, Kind};

/// The aggregate expansion's error for `item`, under `attr`.
fn refused(kind: Kind, attr: Tokens, item: Tokens) -> String {
    match domain(attr, item, kind) {
        Ok(_) => panic!("expanded, but should be refused"),
        Err(error) => error.to_string(),
    }
}

fn lamp(item: Tokens) -> String {
    refused(Kind::Aggregate, quote!(domain = "lamp"), item)
}

#[test]
fn a_second_handler_for_one_type_is_refused() {
    let error = lamp(quote! {
        impl Lamp {
            #[handles_cmd(On)]
            fn on(&self, cmd: On, ctx: &Context) -> Result<Lit, Infallible> { todo!() }
            #[handles_fact(On)]
            fn on_again(&self, fact: On, ctx: &Context) -> Record<Self> { todo!() }
        }
    });
    assert!(error.contains("a second handler for this type"), "{error}");
}

#[test]
fn handlers_that_disagree_on_the_rejection_are_refused() {
    let error = lamp(quote! {
        impl Lamp {
            #[handles_cmd(On)]
            fn on(&self, cmd: On, ctx: &Context) -> Result<Lit, Infallible> { todo!() }
            #[handles_cmd(Off)]
            fn off(&self, cmd: Off, ctx: &Context) -> Result<Dark, WireError> { todo!() }
        }
    });
    assert!(
        error.contains("disagree on the rejection: `WireError` here, `Infallible` before"),
        "{error}"
    );
}

#[test]
fn handlers_that_agree_on_the_rejection_expand() {
    let expanded = domain(
        quote!(domain = "lamp"),
        quote! {
            impl Lamp {
                #[handles_cmd(On)]
                fn on(&self, cmd: On, ctx: &Context) -> Result<Lit, Refused> { todo!() }
                #[handles_cmd(Off)]
                fn off(&self, cmd: Off, ctx: &Context) -> Result<Dark, Refused> { todo!() }
            }
        },
        Kind::Aggregate,
    )
    .expect("expands")
    .to_string();
    assert!(expanded.contains("Rejection < Refused >"), "{expanded}");
}

#[test]
fn the_rejection_is_inferred_through_failed() {
    let expanded = domain(
        quote!(domain = "lamp"),
        quote! {
            impl Lamp {
                #[handles_cmd(On)]
                fn on(&self, cmd: On, ctx: &Context) -> Result<Lit, Failed<Refused>> { todo!() }
                #[handles_cmd(Off)]
                fn off(&self, cmd: Off, ctx: &Context) -> Result<Dark, Refused> { todo!() }
            }
        },
        Kind::Aggregate,
    )
    .expect("expands")
    .to_string();
    assert!(
        expanded
            .contains("type Rejection = :: angzarr_embedded :: markup :: Rejection < Refused >"),
        "{expanded}"
    );
}

#[test]
fn failed_of_another_name_is_not_unwrapped() {
    let error = lamp(quote! {
        impl Lamp {
            #[handles_cmd(On)]
            fn on(&self, cmd: On, ctx: &Context) -> Result<Lit, Failed<Refused>> { todo!() }
            #[handles_cmd(Off)]
            fn off(&self, cmd: Off, ctx: &Context) -> Result<Dark, Unfailed<Refused>> { todo!() }
        }
    });
    assert!(
        error.contains("disagree on the rejection: `Unfailed < Refused >` here, `Refused` before"),
        "{error}"
    );
}

#[test]
fn a_failed_of_two_arguments_is_not_unwrapped() {
    let error = lamp(quote! {
        impl Lamp {
            #[handles_cmd(On)]
            fn on(&self, cmd: On, ctx: &Context) -> Result<Lit, Refused> { todo!() }
            #[handles_cmd(Off)]
            fn off(&self, cmd: Off, ctx: &Context) -> Result<Dark, Failed<Refused, Other>> { todo!() }
        }
    });
    assert!(
        error.contains("disagree on the rejection: `Failed < Refused , Other >` here"),
        "{error}"
    );
}

#[test]
fn a_refusing_handler_may_fail_with_its_choice() {
    domain(
        quote!(domain = "lamp"),
        quote! {
            impl Lamp {
                #[handles_cmd(On, refuses(Burnt))]
                fn on(&self, cmd: On, ctx: &Context) -> Result<Lit, Failed<Refuses<Self, On>>> { todo!() }
            }
        },
        Kind::Aggregate,
    )
    .expect("expands");
}

#[test]
fn a_refusing_handler_failing_with_another_is_refused() {
    let error = lamp(quote! {
        impl Lamp {
            #[handles_cmd(On, refuses(Burnt))]
            fn on(&self, cmd: On, ctx: &Context) -> Result<Lit, Failed<Burnt>> { todo!() }
        }
    });
    assert!(
        error.contains("`Result<_, Failed<Refuses<Self, Cmd>>>`"),
        "{error}"
    );
}

#[test]
fn a_handler_taking_mut_self_is_refused() {
    let error = lamp(quote! {
        impl Lamp {
            #[handles_cmd(On)]
            fn on(&mut self, cmd: On, ctx: &Context) -> Result<Lit, Infallible> { todo!() }
        }
    });
    assert!(error.contains("a handler takes `&self`"), "{error}");
}

#[test]
fn a_handler_taking_self_by_value_is_refused() {
    let error = lamp(quote! {
        impl Lamp {
            #[handles_cmd(On)]
            fn on(self, cmd: On, ctx: &Context) -> Result<Lit, Infallible> { todo!() }
        }
    });
    assert!(error.contains("a handler takes `&self`"), "{error}");
}

#[test]
fn an_applier_taking_self_is_refused() {
    let error = lamp(quote! {
        impl Lamp {
            #[applies(Lit)]
            fn lit(&self, event: Lit) {}
        }
    });
    assert!(error.contains("an applier takes `&mut self`"), "{error}");
}

#[test]
fn a_command_handler_without_a_result_is_refused() {
    let error = lamp(quote! {
        impl Lamp {
            #[handles_cmd(On)]
            fn on(&self, cmd: On, ctx: &Context) -> Option<Lit> { todo!() }
        }
    });
    assert!(
        error.contains("a command handler returns `Result<_, Rejection>`"),
        "{error}"
    );
}

#[test]
fn a_refusing_handler_whose_err_is_not_its_choice_is_refused() {
    let error = lamp(quote! {
        impl Lamp {
            #[handles_cmd(On, refuses(Burnt))]
            fn on(&self, cmd: On, ctx: &Context) -> Result<Lit, Burnt> { todo!() }
        }
    });
    assert!(error.contains("`Result<_, Refuses<Self, Cmd>>`"), "{error}");
}

#[test]
fn a_fact_handler_returning_a_result_is_refused() {
    let error = lamp(quote! {
        impl Lamp {
            #[handles_fact(Cut)]
            fn cut(&self, fact: Cut, ctx: &Context) -> Result<Dark, Infallible> { todo!() }
        }
    });
    assert!(
        error.contains("a fact handler returns no `Result`"),
        "{error}"
    );
}

#[test]
fn a_fact_handler_returning_an_emit_is_refused() {
    let error = lamp(quote! {
        impl Lamp {
            #[handles_fact(Cut)]
            fn cut(&self, fact: Cut, ctx: &Context) -> Emit<Self> { todo!() }
        }
    });
    assert!(error.contains("not an `Emit`"), "{error}");
}

#[test]
fn an_on_refused_handler_returning_no_result_is_refused() {
    let error = lamp(quote! {
        impl Lamp {
            #[on_refused(On)]
            fn on_refused(&self, refused: CommandRefused<On>, ctx: &Context) -> Record<Self> { todo!() }
        }
    });
    assert!(
        error.contains("an `#[on_refused]` handler returns `Result<Record<Self>, E>`"),
        "{error}"
    );
}

#[test]
fn an_on_refused_handler_returning_an_emit_is_refused() {
    let error = lamp(quote! {
        impl Lamp {
            #[on_refused(On)]
            fn on_refused(&self, refused: CommandRefused<On>, ctx: &Context) -> Result<Emit<Self>, Report> { todo!() }
        }
    });
    assert!(
        error.contains("an `#[on_refused]` handler records a `Record`, not an `Emit`"),
        "{error}"
    );
}

#[test]
fn starts_on_an_aggregate_is_refused() {
    let error = lamp(quote! {
        impl Lamp {
            #[handles_cmd(On, starts)]
            fn on(&self, cmd: On, ctx: &Context) -> Result<Lit, Infallible> { todo!() }
        }
    });
    assert!(error.contains("`starts` is a process manager's"), "{error}");
}

#[test]
fn starts_on_an_applier_is_refused() {
    let error = refused(
        Kind::ProcessManager,
        quote!(domain = "timer"),
        quote! {
            impl Timer {
                #[applies(Rang, starts)]
                fn rang(&mut self, event: Rang) {}
            }
        },
    );
    assert!(
        error.contains("`starts` marks a process manager's command or fact handler only"),
        "{error}"
    );
}

#[test]
fn refuses_on_a_fact_handler_is_refused() {
    let error = lamp(quote! {
        impl Lamp {
            #[handles_fact(Cut, refuses(Burnt))]
            fn cut(&self, fact: Cut, ctx: &Context) -> Record<Self> { todo!() }
        }
    });
    assert!(error.contains("a fact is never refused"), "{error}");
}

#[test]
fn a_process_manager_with_only_refusal_handlers_still_skips_for_no_instance() {
    let expanded = domain(
        quote!(domain = "timer"),
        quote! {
            impl Timer {
                #[on_refused(On)]
                fn on_refused(&self, refused: CommandRefused<On>, ctx: &Context) -> Result<Record<Self>, Infallible> { todo!() }
            }
        },
        Kind::ProcessManager,
    )
    .expect("expands")
    .to_string();
    // The kernel skips a fact for a stream with no instance, of a type that
    // starts none: the expansion routes the refusal and declares no starting
    // input.
    assert!(expanded.contains(". on_refused ("), "{expanded}");
    assert!(
        expanded.contains(
            "const STARTS : & 'static [:: angzarr_embedded :: markup :: TypeName] = & [] ;"
        ),
        "{expanded}"
    );
}

#[test]
fn a_flow_argument_that_is_not_key_value_is_refused() {
    let error = flow(
        quote!(name = "s", source = A, target = B, translate(On)),
        quote!(impl S {}),
        Flow::Policy,
    )
    .expect_err("refused")
    .to_string();
    assert!(
        error.contains("translate mode is its `#[on_refused(Cmd)]`"),
        "{error}"
    );
}

#[test]
fn an_unknown_flow_argument_names_the_usage() {
    let policy = flow(
        quote!(name = "s", source = A, sink = B),
        quote!(impl S {}),
        Flow::Policy,
    )
    .expect_err("refused")
    .to_string();
    assert!(policy.contains("#[policy] takes `name"), "{policy}");
    let projector = flow(
        quote!(name = "p", target = A),
        quote!(impl P {}),
        Flow::Projector,
    )
    .expect_err("refused")
    .to_string();
    assert!(
        projector.contains("#[projector] takes `name"),
        "{projector}"
    );
}

/// A projector's sources are a list, each a domain type or a name: one
/// `source`, a bare type, an empty list and anything else in the list are
/// refused.
#[test]
fn a_projectors_sources_are_a_list_of_domains() {
    let refused = |attr: Tokens| {
        flow(attr, quote!(impl P {}), Flow::Projector)
            .expect_err("refused")
            .to_string()
    };
    for attr in [
        quote!(name = "p", source = A),
        quote!(name = "p", sources = A),
        quote!(name = "p", sources = [1]),
    ] {
        let error = refused(attr.clone());
        assert!(
            error.contains("`sources = [<domain type"),
            "{attr}: {error}"
        );
    }
    let error = refused(quote!(name = "p", sources = []));
    assert!(error.contains("at least one source domain"), "{error}");
}

/// Each source is its name: a string as written, a type as its
/// `Domain::NAME`, in order.
#[test]
fn a_projectors_sources_are_named_in_order() {
    let expanded = flow(
        quote!(name = "p", sources = [Wallet, "lamp"]),
        quote!(impl P {}),
        Flow::Projector,
    )
    .expect("expanded")
    .to_string();
    let wallet = quote!(<Wallet as ::angzarr_embedded::Domain>::NAME).to_string();
    let sources =
        format!("const SOURCES : & 'static [& 'static str] = & [{wallet} , \"lamp\" ,] ;");
    assert!(expanded.contains(&sources), "{expanded}");
}
