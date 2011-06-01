package org.apache.zookeeper.recipes.tpcp;

/**
 * Defines transaction's states
 *
 */
enum TransactionState {
    /**
     * First common step on all transactions, just when constructor arrives
     */
    PRESET,

    /**
     * All set, waiting for votes (coordinator) or waiting local vote (participant)
     */
    SET,

    // TODO define coordinator states

    /**
     * Aborted.
     */
    ABORTED,

    /**
     * Committed locally
     */
    PRE_COMMITTED,

    /**
     * Fully committed on all peers.
     */
    COMMITTED;

    public static TransactionState parse(String state) {
        return TransactionState.valueOf(state);
    }
}
