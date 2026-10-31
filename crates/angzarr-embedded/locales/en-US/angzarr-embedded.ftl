# Messages for the angzarr-embedded crate's errors. Each id is an error's code; the
# variables are its args. Messages are lowercase fragments, like the English
# Display, so a nested cause reads well inside another message.

## The audited kernel, angzarr_embedded::next. $stream names a stream as domain/root,
## with @edition outside the main timeline.

angzarr-embedded-malformed-no-cover = the book has no cover
angzarr-embedded-malformed-unknown-domain = no domain "{ $domain }" is registered
angzarr-embedded-malformed-no-root = the cover has no root
angzarr-embedded-malformed-bad-root = the root is { $len ->
        [one] one byte
       *[other] { $len } bytes
    }, not a UUID's 16
angzarr-embedded-malformed-empty-edition = the edition is set but empty: the main timeline is the unset edition
angzarr-embedded-malformed-no-page = the book has no page
angzarr-embedded-malformed-no-body = the page has neither a command nor a fact
angzarr-embedded-malformed-no-payload = the command or fact has no payload
angzarr-embedded-malformed-bad-command-id = the command id is { $len ->
        [one] one byte
       *[other] { $len } bytes
    }, not a UUID's 16
angzarr-embedded-malformed-bad-correlation-id = the correlation id is { $len ->
        [one] one byte
       *[other] { $len } bytes
    }, not a UUID's 16
angzarr-embedded-malformed-causation-from-outside = a client's book names a causation: only the kernel's chain issues policy commands
angzarr-embedded-malformed-id-on-policy-command = a policy's book names its own { $field }: the kernel derives its ids from the trigger
angzarr-embedded-malformed-fact-from-policy = a policy's book holds a fact: a policy never injects one
angzarr-embedded-malformed-handling-on-policy-command = a policy's book has handling: its target stamps its own head
angzarr-embedded-malformed-bad-trigger = the policy command's trigger cannot name a command id: { $reason ->
        [bad-root] its root is not a UUID's 16 bytes
        [missing-trigger] it needs a stream with a root
        [empty-edition] its stream's edition is set but empty
        [empty-component] the reacting component has no name
       *[other] { $reason }
    }
angzarr-embedded-malformed-fact-without-origin = a fact without its origin
angzarr-embedded-malformed-empty-external-id = the fact's external id is empty: it is the fact's identity
angzarr-embedded-malformed-command-with-fact-origin = a command with a fact origin
angzarr-embedded-malformed-fact-command-id-not-derived = the fact's command id is not the one derived from its external id
angzarr-embedded-malformed-handling-on-fact = a fact with handling: a fact is never checked against a sequence nor conflict-resolved
angzarr-embedded-malformed-unknown-conflict-resolution = conflict resolution { $value } is not one this build knows
angzarr-embedded-malformed-unsupported-conflict-resolution = domain "{ $domain }"'s state does not support { $requested }
angzarr-embedded-malformed-refusal-misaddressed = the refusal fact is for another stream than the one its trigger names

angzarr-embedded-projection-failed = the command committed, then { $failures ->
    [one] projector { $projector } failed: { $error }
   *[other] { $failures } projectors failed, the first { $projector }: { $error }
}
angzarr-embedded-downstream = a policy command down the chain failed, at depth { $depth }: { $error }
angzarr-embedded-stale = stream { $stream } is at { $actual }, not the { $expected } expected: re-read and re-issue
angzarr-embedded-expected-sequence-ahead = stream { $stream } is at { $head }, behind the { $expected } expected: the command names a sequence the stream never reached
angzarr-embedded-overlap = the window of stream { $stream } from { $expected } to { $actual } changed { $window_units }, which the command's change to { $command_units } overlaps: re-read and re-issue
angzarr-embedded-retry-cap-exhausted = gave up on stream { $stream } after { $attempts ->
        [one] one run
       *[other] { $attempts } runs
    }, each losing its write to another write to the stream, at the cap of { $cap }: dead-lettered, nothing persisted
angzarr-embedded-rules-version-mismatch = the handler of stream { $stream } answered as rules version "{ $reported }", not "{ $registered }", the version it is registered at: a bug in the handler; nothing was kept
angzarr-embedded-unresolved = decide never acknowledged the window of stream { $stream } from { $expected } to { $actual }
angzarr-embedded-unknown-type = stream { $stream }'s domain does not decode the type { $type_url }
angzarr-embedded-invalid-payload = a { $type_url } for stream { $stream } is not a valid value: { $reason }
angzarr-embedded-upcast = upcasting a payload for stream { $stream } failed: { $error }
angzarr-embedded-wrong-domain = stream { $stream } was given to the { $expected } domain's code
angzarr-embedded-chain-too-deep = the command chain went deeper than its limit of { $limit }
angzarr-embedded-step-limit = the decision on stream { $stream } ran more than its limit of { $limit } continuation steps
angzarr-embedded-before-snapshot = stream { $stream } holds its events from { $first } on only, after its snapshot
angzarr-embedded-unregistered-domain = a stored { $domain } stream was read, but no domain "{ $domain }" is registered
angzarr-embedded-handler-unavailable = the handler of stream { $stream } could not be reached ({ $cause }): run it again

angzarr-embedded-replay-out-of-sequence = page { $index } of the log claims sequence { $found }, not the one due
angzarr-embedded-replay-recorded-backwards = event { $sequence } was recorded before the event before it
angzarr-embedded-replay-incomplete-meta = event { $sequence }'s metadata lacks a well-formed { $field }
angzarr-embedded-replay-wrong-domain = a { $found } log was given to the { $expected } domain's code

angzarr-embedded-unreadable = event { $sequence } of stream { $stream } cannot be replayed: { $reason }

angzarr-embedded-payload-unknown-type = no decoder for the type { $type_url }
angzarr-embedded-payload-decode = a payload of type { $type_url } does not decode
angzarr-embedded-payload-invalid = a payload of type { $type_url } is not a valid value: { $reason }
angzarr-embedded-wire-missing = { $message }.{ $field } is required and absent
angzarr-embedded-wire-unknown-enum = { $message }.{ $field } holds { $value }, which no variant defines
angzarr-embedded-upcast-unavailable = the upcaster sidecar is not serving ({ $cause }): retry
angzarr-embedded-upcast-rule = the upcast rule from { $source } failed: { $error }
angzarr-embedded-upcaster-duplicate = domain { $domain }'s upcaster has two typed rules from { $source }: the second never runs
angzarr-embedded-upcaster-out-of-order = domain { $domain }'s upcaster registers the rule from { $source } before the rule from { $produced_by }, which produces it
angzarr-embedded-upcaster-dead-end = domain { $domain }'s upcaster takes { $source } to { $ends_at }, which the domain does not decode

angzarr-embedded-misconfigured-already-registered = domain "{ $domain }" is registered already
angzarr-embedded-misconfigured-upcaster-for-another-domain = the upcaster registered for domain "{ $domain }" is for "{ $upcaster }"
angzarr-embedded-misconfigured-empty-rules-version = domain "{ $domain }"'s current rules version is empty
angzarr-embedded-misconfigured-bad-state-descriptor = domain "{ $domain }"'s state descriptor lists a value that is not a resolution, or lacks STRICT or AGGREGATE_HANDLES
angzarr-embedded-misconfigured-unsupported-resolution = domain "{ $domain }" is configured { $resolution }, which its state does not support
angzarr-embedded-misconfigured-unsupported-command-resolution = domain "{ $domain }" configures { $command_type } { $resolution }, which its state does not support
angzarr-embedded-misconfigured-remote-source = "{ $component }" handles the events of domain "{ $domain }", which is remote: its events are its coordinator's, never this process's

# angzarr-router's refusals, one code per reason.
angzarr-embedded-any-decode-failed = a payload did not decode as the type it names: send a payload of its type
angzarr-embedded-persisted-event-corrupt = a stored event or snapshot does not decode: restore it, or add the upcaster it needs
angzarr-embedded-unhandled-handler-error = a handler failed with an error it does not classify: a bug in the handler
angzarr-embedded-no-handlers-registered = no component is registered: register one
angzarr-embedded-no-fact-handler = no handler is registered for this fact's type: register one, or stop sending it
angzarr-embedded-conflict-resolution-unsupported = the domain does not support the conflict resolution asked for: ask for another

# angzarr-router's reasons (decision 14.37), each under its own code.
angzarr-embedded-handler-panicked = a handler panicked: a bug in the handler
angzarr-embedded-command-refused = the command was refused with a typed refusal
angzarr-embedded-fold-failed = a stored event could not be folded into the state: a bug, or a missing upcaster
angzarr-embedded-fold-failed-emitted = an event the command emitted could not be folded into the state: a bug in the domain
angzarr-embedded-unknown-command-type = no handler takes this command's type, after upcasting: a bug in the sender
angzarr-embedded-no-refusal-handler = nothing handles the refusal of this command: give its source a refusal handler
angzarr-embedded-no-component-of-kind = the business process hosts no component of the kind dispatched to
angzarr-embedded-duplicate-component = a router hosts one component: register the other in its own
angzarr-embedded-too-many-steps = a decision ran more continuation steps than its limit: a step that always continues, a bug in the domain

# The coordinator's keys (CoordinatorReason, reasons.proto) the kernel
# answers, each under its own code.
angzarr-embedded-conflict-unresolved = the handler never acknowledged the events it did not see: a bug in the handler; re-read and re-issue
angzarr-embedded-conflict-overlap = the command's changes overlap changes it did not see: re-read and re-issue
angzarr-embedded-policy-command-outside-destination = a policy sent a command to a domain other than its one destination: a bug in the policy
angzarr-embedded-policy-command-outside-edition = a policy sent a command into an edition other than its trigger's: a bug in the policy
angzarr-embedded-policy-command-no-root = a policy sent a command naming no root: a bug in the policy, which names the root of every command
angzarr-embedded-policy-command-other-correlation = a policy sent a command in a correlation other than its trigger's: a bug in the policy
angzarr-embedded-policy-command-with-handling = a policy sent a command carrying an expected sequence or a conflict resolution, which only its target sets: a bug in the policy
angzarr-embedded-policy-command-other-identity = a policy sent a command naming a command id or causation other than the one its trigger stamps: a bug in the policy
angzarr-embedded-policy-command-is-fact = a policy sent a fact, where it sends commands only: a bug in the policy, whose refusal facts come from its refusal handler
angzarr-embedded-policy-command-fact-origin = a policy sent a command carrying a fact origin, which only a fact carries: a bug in the policy
angzarr-embedded-pm-no-instance = the process manager has no instance here, and only a starting input starts one: start it first
angzarr-embedded-refusal-fact-wrong-stream = a refusal fact was addressed to a stream other than its trigger's: a bug in the runtime
angzarr-embedded-refusal-fact-addressed = a policy set its refusal fact's cover or handling: a bug in the policy
angzarr-embedded-refusal-fact-no-page = a policy's refusal fact has no page: a bug in the policy
angzarr-embedded-refusal-fact-not-a-fact = a policy answered a refusal with a command, not a fact: a bug in the policy
angzarr-embedded-refusal-fact-identified = a policy set its refusal fact's identity: a bug in the policy
