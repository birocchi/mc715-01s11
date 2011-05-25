package zk_lock.imp.client;

import org.apache.zookeeper.recipes.lock.LockListener;

public class LockListner implements LockListener
{
	private String lockPath;
	private Client c;
	
	public LockListner(Client c, String lockPath)
	{
		this.c = c;
		this.lockPath = lockPath;
	}
	
	@Override
	public void lockAcquired() 
	{
		c.addListnerMessage("Lock " + lockPath + " has been acquired.");		
	}

	@Override
	public void lockReleased() 
	{
		c.addListnerMessage("Lock " + lockPath + " has been freed.");
	}    
}
