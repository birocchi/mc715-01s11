package zk_lock.imp.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.recipes.lock.WriteLock;

public class Client extends Thread implements Watcher
{
    private ZooKeeper zk;
    private volatile boolean dead;
    private LinkedList<WriteLock> lockList;
	private LinkedList<String> listnerMessages;
    
    /* properties */	
    public boolean getDead()
    {
    	return dead;
    }
    
    public void addListnerMessage(String msg)
    {
    	listnerMessages.add(msg);
    }

    /* constructor */
	
    public Client(String[] hosts, int port) throws IOException
    {
		StringBuilder connString = new StringBuilder();
			
		for(String h : hosts)
		    connString.append(h + ":").append(port).append(",");
			
		connString.setLength(connString.length() - 1);		

		dead = false;		
		zk = new ZooKeeper(connString.toString(), 3000, this);
		lockList = new LinkedList<WriteLock>(); 
		listnerMessages = new LinkedList<String>();
    }
		
    @Override
	public void process(WatchedEvent event)
    {	
		if (event.getType() == Event.EventType.None)
	    {
			// estado da conexao mudou
			switch(event.getState())
		    {
			    case SyncConnected:
					// conectamos
					// podemos iniciar					
			    	break;

			    case Expired:
					// sessao expirada
					dead = true;
					break;
		    }
	    }
		else
	    {
			// algo no nó mudou
	    }
    }
	
    /**
     * Starts this client.
     */
    public void run()
    {				
		BufferedReader stdin = new BufferedReader(new java.io.InputStreamReader(System.in));
		String line;

		System.out.println("Waiting connection...");
					
		try
	    {
			while (zk.getState() != States.CONNECTED)
			    sleep(500);
	    } 
		catch (InterruptedException e1)
	    {
			e1.printStackTrace();
	    }

		System.out.println("Connection established.");
		
		bash();
		
		try
	    {
			while (true)
		    {
				if (!listnerMessages.isEmpty())
				{
					System.out.println();
					
					for(String msg : listnerMessages)
						System.out.println("LockListner: " + msg);
					
					listnerMessages.clear();
					
					bash();
				}
					
				if (stdin.ready())
				{
					line = stdin.readLine().toLowerCase();
					
					String[] args = line.split(" "); 
					String cmd = args[0];								
					
					if (args.length == 0)
				    {
						usage();
				    }
					else if (args.length == 1)
					{					
						if(cmd.equals("owner"))
					    {
							owner();
					    }
						else if (cmd.equals("quit"))
						{
							break;
						}
						else
						{
							usage();					
						}
					}
					else if (args.length == 2)
					{
						String arg = args[1];
						
						if (arg.charAt(0) != '/')
						    System.out.println("Lock must start with /");
						else
						{
							if (cmd.equals("lock"))
						    {					
								lock(arg);
						    }
							else if (cmd.equals("unlock"))
						    {
								unlock(arg);
						    }
							else if (cmd.equals("ls"))
						    {
								ls(arg);
						    }
							else
								usage();
					    }
					}
					
					bash();
				}
		    }
	    } 
		catch (IOException e)
	    {
			e.printStackTrace();
	    }
		
		try 
		{
			zk.close();
		} 
		catch (InterruptedException e) 
		{
			e.printStackTrace();
		}
		
		dead = true;
    }
    
    private void bash()
    {
    	String myID = String.valueOf(zk.getSessionId());
    	System.out.print("[client-" + myID + "@zookeper]$ ");
    }
	
	private void usage()
    {
    	System.out.println("Usage:");
		System.out.println("lock /lockname - try to get lock 'lockname'");
		System.out.println("unlock /lockname - free lock 'lockname'");
		System.out.println("ls /path - show lock list on 'lockname'");
		System.out.println("owner - show current pending or held locks");
		System.out.println("quit - exits");
    }
    
	private void ls(String arg) 
    {
		try
		{
		    List<String> children = zk.getChildren(arg, false);
		    for (String cname : children)
				System.out.println(cname);
		} 
		catch (Exception e)
		{
		    e.printStackTrace();
		}		
	}
	
	private void owner()
	{
		if (lockList.isEmpty())
			System.out.println("You don't have or are waiting on any lock.");

		for(WriteLock wl : lockList)
		{
			System.out.print(wl.getId());
			
			if (wl.isOwner())
				System.out.println(" HELD");
			else
				System.out.println(" WAITING");
		}
	}
	
    private WriteLock getLock(String lockpath)
    {
    	for(WriteLock wl : lockList)
    		if (wl.getDir().equals(lockpath))
    			return wl;
    	
    	return null;
    }
    
    private void unlock(String arg)
    {
    	WriteLock wl = getLock(arg);
    	
		if (wl == null)
			System.out.println("You don't hold (or are waiting on) lock " + arg);
		else
		{
			if (wl.isOwner())
				System.out.print("I own lock " + arg + ". Freeing lock...");
			else
				System.out.print("I DON'T own lock " + arg + ". Leaving wait state...");
			
			wl.unlock();
			
			lockList.remove(wl);
			
			System.out.println("DONE");
		}
    }

    private void lock(String arg)
    {
    	WriteLock wl = getLock(arg);
			
		if (wl != null)
		{
			if (wl.isOwner())
				System.out.println("You already hold lock " + arg);
			else
				System.out.println("You are already waiting for lock " + arg);
		}
		else
	    {
			LockListner ll = new LockListner(this, arg);
			wl = new WriteLock(zk, arg, null, ll);
			lockList.add(wl);
			
			try
		    {
				System.out.print("Trying to get lock " + arg + "...");
					
				if (wl.lock())
				    System.out.println("LOCK ACQUIRED");
				else
				    System.out.println("WAITING ON LOCK");
		    }
			catch (KeeperException e)
		    {
				e.printStackTrace();
		    } 
			catch (InterruptedException e)
		    {
				e.printStackTrace();
		    }
	    }
    }
}
