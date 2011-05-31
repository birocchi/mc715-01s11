package org.apache.zookeeper.recipes.tpcp;

import java.io.Serializable;
import java.util.Hashtable;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;

/**
 * Transaction viewed by the coordinator
 *
 */
class CoordinatorTransaction extends BaseTransaction 
{
	private Object syncLock;
	private Hashtable<String, TransactionState> participants;
	
	/**
	 * How many participants have already voted.
	 * **** ATENTION: Change this only when in sync syncLock ****
	 */
	private long participantVotes;
	
	/**
	 * How many participants have already joined this transaction
	 * **** ATENTION: Change this only when in sync syncLock ****
	 */
	private long participantsGreeted;
	
	/**** ATENTION: Change this.state only when in sync syncLock ****/
	
	public CoordinatorTransaction(Serializable query, GroupMember coordinator,
			List<GroupMember> participants) throws InterruptedException 
	{
		super(query, coordinator, participants);
		
		this.participants = new Hashtable<String, TransactionState>(participants.size());
		
		// initialize all participants with 'preset' state to indicate that they have not yet joined the transaction
		for (GroupMember gm : participants)
			this.participants.put(gm.getName(), TransactionState.PRESET);
		
		// we assume coordinator has already pre-commited
		this.participants.put(me.getName(), TransactionState.PRE_COMMITTED);
		this.participantVotes = 1;
		this.participantsGreeted = 1;
		
		this.syncLock = new Object();
	}
	
	private void updateParticipantState(String participantID) throws InterruptedException
	{
		TransactionState oldState;
		TransactionState newState;
		
		synchronized (this.syncLock)
		{
			// if our (coordinator's) state is SET, there's no decision yet
			// may there be a case where coordinator have already decided, but there's still votes not seen
			// in that case, we can safely ignore new votes
			if (state != TransactionState.SET)
			{
				return;
			}
			
			 oldState = participants.get(participantID);
		}

		if (oldState == null)
		{			
			System.err.println("Invalid participant '"+participantID+"' on transaction '"+zNodePath+"'");
			return;
		}
		else if (oldState == TransactionState.ABORTED || oldState == TransactionState.PRE_COMMITTED)
		{
			// the watcher was left alone - we should ignore this
			// or we are looking at children list and getting new joined participants while others have already voted 
			// TODO: improve left alone watcher - i.e. find a way to avoid setting watcher when it's not really needed
			return;
		}
		
		// from this line, the participant oldState is one of { PRESET, SET } and newState is possibly one of { SET, PRE_COMMITED, ABORTED }   
		
		// it's up to me to retrieve the znode state
		// we can't a priori know the participant actual state thus we can't know if we should set a watch or not
		// TODO: there's an issue here: left alone watcher - find a way to avoid the last watch (which is left alone)
		String voteData = readParticipantNode(getPath(participantID), true);
		
		if (voteData == null)
		{
			// damn! participant crashed (ephemeral node was deleted)
			// we should abort
			newState = TransactionState.ABORTED;
		}
		else
			newState = TransactionState.parse(voteData);

		synchronized (this.syncLock)
		{
			// old state maybe out sync
			oldState = participants.get(participantID);
			
			if (oldState == TransactionState.PRESET || oldState == TransactionState.SET)
			{
				participants.put(participantID, newState);
				
				// if this is the first time a participant shows up, we should count him in
				if (oldState == TransactionState.PRESET)
					participantsGreeted++;
			}
			
			// if someone casts a vote, we should see if we can reach a decision
			
			// if our (coordinator's) state is SET, there's no decision yet 
			if (state == TransactionState.SET)
			{
				try
				{
					if (newState == TransactionState.PRE_COMMITTED)
					{
						// have to wait for other's votes
						// let's update number of decided nodes
						participantVotes++;
											
						// if everyone has voted, we can decide
						if (participantVotes == participants.size())
						{
							commit(false);
							finish();
						}
					}
					else if (newState == TransactionState.ABORTED)
					{
						// can decide already
						abort();			
						finish();
					}
				}
				catch(Exception e)
				{
					System.err.println("Double decision, prog. error");
				}
			}
		}
	}

	/**
	 * Greets new participants
	 * @return true if every expected participant was greeted at the end of this execution
	 * @throws InterruptedException
	 */
	private boolean greetNewParticipants() throws InterruptedException
	{
		boolean everyOneIn = false;
		
		try
		{
			List<String> children = zkClient.getChildren(zNodePath, getDefaultWatcher());
			
			// avoid multiple watcher calls to collide
			synchronized (this.syncLock)
			{
				for (String c : children)
					updateParticipantState(c);
				
				everyOneIn = participantsGreeted == participants.size();
			}
		}
		catch (KeeperException e)
		{
			// TODO handle this
		}
		
		return everyOneIn;
	}
	
	private void coordinate() throws InterruptedException
	{
		// welcome new participants that join transaction
		greetNewParticipants();
		
		// watcher events will do the rest		
	}
	
	@Override
	public void run()
	{
		try
		{
			coordinate();
		} 
		catch (InterruptedException e)
		{
			// TODO handle this
		}
	}
	
	private void finish()
	{
		// free what we don't need anymore
		participants = null;
		
		decisionReached();
	}

	@Override
	protected void nodeEvent(WatchedEvent event)
	{
		// coordinator watches for new participants to join and for votes cast
		
		String fullPath = event.getPath();
		String path = fullPath.substring(fullPath.lastIndexOf('/') + 1);
		
		EventType etype = event.getType();
		
		try
		{
			if (path.length() == 0)
			{
				// it's transaction root znode
				// get children changed event
				if (etype == EventType.NodeChildrenChanged)
				{
					greetNewParticipants();					
				}
			}
			else
			{
				// it's a participant node
				// get data changed event
				if (etype == EventType.NodeDataChanged)
				{
					updateParticipantState(path);
				}
			}
		}
		catch(InterruptedException e)
		{
			// TODO some thing, this is bad :(
		}
	}
}
