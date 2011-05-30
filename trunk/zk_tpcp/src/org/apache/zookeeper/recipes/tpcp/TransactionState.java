package org.apache.zookeeper.recipes.tpcp;

/**
 * Defines transaction's state
 *
 */
enum TransactionState
{
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
	ABORTED
	{
		@Override
		public String toString()
		{
			return "a";
		}
	},
	
	/**
	 * Committed locally
	 */
	PRE_COMMITTED,
	
	/**
	 * Fully committed on all peers.
	 */
	COMMITTED
	{
		@Override
		public String toString()
		{
			return "c";
		}
	},
}
