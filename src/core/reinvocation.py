"""Capped-run self-reinvocation decision.

Pure component — no AWS dependencies.

A run that ends as a Capped_Run (the journal-read row cap was hit, so a
backlog remains) triggers the next run immediately via an async self-invoke
rather than waiting for the next scheduled trigger, raising the Solution's
sustained Drain_Ceiling. ``should_reinvoke`` is the pure decision of whether
that trigger should fire; the handler performs the actual I/O (the async
invoke) based on its result.

Requirements: 4.1, 4.2, 4.3, 5.1, 5.4
"""
from __future__ import annotations


def should_reinvoke(
    capped: bool,
    progressed: bool,
    depth: int,
    chain_limit: int,
    bucket_active: bool,
) -> bool:
    """Return whether a Self_Reinvocation should be triggered.

    Returns ``True`` iff all of the following hold:

    - ``capped`` — the run was a Capped_Run (the journal-read row cap was hit,
      so a backlog remains for the window) (Requirement 4.1).
    - ``progressed`` — the run submitted its Batch_Replication_Job successfully
      and the checkpoint advanced; a failing run must never storm reinvocations
      (Requirement 4.3).
    - ``bucket_active`` — the bucket is not disabled and its circuit breaker
      has not tripped (Requirement 5.4).
    - ``depth < chain_limit`` — the chain has not yet reached the configured
      ``Reinvocation_Chain_Limit`` (Requirement 5.1).

    A run that fully drained its window (``capped`` is ``False``) must not
    reinvoke (Requirement 4.2).

    Pure and total: every argument combination, including negative or
    boundary ``depth``/``chain_limit`` values, produces a deterministic
    ``bool`` with no exceptions raised.

    Parameters
    ----------
    capped:
        Whether the run was a Capped_Run.
    progressed:
        Whether the run submitted a job and advanced its checkpoint.
    depth:
        The current ``reinvocation_depth`` carried by this invocation (0 for
        a scheduled trigger).
    chain_limit:
        The configured ``Reinvocation_Chain_Limit``.
    bucket_active:
        Whether the bucket is enabled and its circuit breaker has not
        tripped.

    Returns
    -------
    bool
        ``True`` iff a Self_Reinvocation should be issued.

    Examples
    --------
    >>> should_reinvoke(True, True, 0, 20, True)
    True
    >>> should_reinvoke(False, True, 0, 20, True)
    False
    >>> should_reinvoke(True, False, 0, 20, True)
    False
    >>> should_reinvoke(True, True, 20, 20, True)
    False

    Requirements: 4.1, 4.2, 4.3, 5.1, 5.4
    """
    return capped and progressed and bucket_active and depth < chain_limit
