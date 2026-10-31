# Messages for angzarr-embedded-host's errors. Each id is an error's code; the
# variables are its args. Messages are lowercase fragments, like the English
# Display, so they read well inside another message.

angzarr-embedded-host-handshake-malformed = malformed handshake "{ $line }": expected angzarr-handshake v1 unix:<absolute path>
angzarr-embedded-host-handshake-timeout = no handshake within { $timeout_ms } ms
angzarr-embedded-host-handshake-closed = stdout closed before a handshake
angzarr-embedded-host-handshake-unreadable = stdout could not be read ({ $kind })
angzarr-embedded-host-duplicate-domain = component "{ $domain }" is configured twice
angzarr-embedded-host-no-launch = component "{ $domain }": language "{ $language }" has no default launch command; give one
angzarr-embedded-host-unknown-language = the config's "{ $key }" names language "{ $language }", which is not loaded; the languages loaded are { $loaded }
angzarr-embedded-host-empty-launch = component "{ $domain }": the launch command is empty
angzarr-embedded-host-spawn = component "{ $domain }": could not start "{ $program }" ({ $kind })
angzarr-embedded-host-handshake = component "{ $domain }": { $cause }
angzarr-embedded-host-connect = component "{ $domain }": could not connect to { $socket }
angzarr-embedded-host-describe = component "{ $domain }": Describe failed with gRPC code { $grpc_code }
angzarr-embedded-host-health = component "{ $domain }": its gRPC health watch failed with gRPC code { $grpc_code }
angzarr-embedded-host-kind-mismatch = component "{ $domain }" is configured as a { $configured } but describes itself as { $described }
angzarr-embedded-host-no-input = component "{ $domain }" names no input domain in its descriptor
angzarr-embedded-host-upcaster-no-domain = component "{ $domain }" is an upcaster that names no domain to upcast: give it `upcasts`
angzarr-embedded-host-not-exposable = component "{ $domain }" is a { $kind }: only an aggregate may be exposed
angzarr-embedded-host-not-exposed = a request for "{ $domain }" from { $arrival } was answered as an unknown domain: its component is exposed { $expose }
angzarr-embedded-host-descriptor = component "{ $domain }": its descriptor is refused: { $cause }
angzarr-embedded-host-descriptor-changed = component "{ $domain }" restarted describing another component than it is registered as: restart the host to take the change
angzarr-embedded-host-runtime = the host's runtime could not be built ({ $kind })
angzarr-embedded-host-socket-dir-too-long = socket_dir "{ $dir }" is too long for a Unix socket path: at most { $max_bytes ->
        [one] { $max_bytes } byte
       *[other] { $max_bytes } bytes
    }
angzarr-embedded-host-config-unreadable = the config { $path } could not be read ({ $kind })
angzarr-embedded-host-config-invalid = the config { $path } is not valid: { $cause }
angzarr-embedded-host-not-registered = component "{ $domain }" is listed as an in-process { $kind }, but the application registered none
angzarr-embedded-host-not-listed = the application registered the in-process { $kind } "{ $domain }", but no component entry lists it
angzarr-embedded-host-register = a component could not be registered: { $cause }
angzarr-embedded-host-store = the store could not be opened: { $cause }
angzarr-embedded-host-descriptor-missing-rules-version = component "{ $component }": its descriptor has no rules version
angzarr-embedded-host-descriptor-starting-types-on-non-pm = component "{ $component }": a { $component_kind } lists starting types; only a process manager may
angzarr-embedded-host-descriptor-missing-state-type = component "{ $component }": a { $component_kind } has no state type
angzarr-embedded-host-descriptor-state-on-stateless-kind = component "{ $component }": a { $component_kind } has a state; it may not
angzarr-embedded-host-descriptor-policy-input-count = component "{ $component }": a policy has { $input_count ->
        [one] { $input_count } input
       *[other] { $input_count } inputs
    }; it must have exactly one
angzarr-embedded-host-descriptor-policy-no-output-domain = component "{ $component }": a policy has no output domain
angzarr-embedded-host-descriptor-output-domain-on-non-policy = component "{ $component }": a { $component_kind } has an output domain; only a policy may
