//! The deprecated `next` alias: the paths an angzarr-cli that predates the
//! flattening writes into generated code (`::angzarr_embedded::next::…`)
//! still name the crate root's items, until the cli is re-pinned
//! (architecture-fixes B4) and the alias goes.

#[test]
fn the_old_next_paths_name_the_crate_roots_items() {
    let _: Option<::angzarr_embedded::next::KernelError> = None::<::angzarr_embedded::KernelError>;
    let _: Option<::angzarr_embedded::next::WireError> = None::<::angzarr_embedded::WireError>;
    let _: Option<::angzarr_embedded::next::markup::Context> =
        None::<::angzarr_embedded::markup::Context>;
    let _: Option<::angzarr_embedded::next::v1::Cover> = None::<::angzarr_embedded::v1::Cover>;
}
