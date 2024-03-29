package zk_tpcp.imp.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.recipes.tpcp.ITransaction;
import org.apache.zookeeper.recipes.tpcp.TransactionGroup;

public class Client extends Thread implements Watcher
{
    private ZooKeeper zk;
    private volatile boolean dead;
	private LinkedList<String> listnerMessages;
	private ArrayList<TransactionGroup> groupList;
	
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
		listnerMessages = new LinkedList<String>();
		groupList = new ArrayList<TransactionGroup>();
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
		
		while (true)
	    {
			try
			{
				Thread.sleep(500);
			}
			catch(Exception e)
			{
				break;
			}
			
			synchronized (System.in)
			{
				try
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
							if (cmd.equals("quit"))
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
							    System.out.println("Path must start with /");
							else
							{
								if (cmd.equals("join"))
							    {					
									join(arg);
							    }
								else if (cmd.equals("leave"))
							    {
									leave(arg);
							    }
								else if (cmd.equals("ls"))
							    {
									ls(arg);
							    }
								else if (cmd.equals("rm"))
								{
									rm(arg);
								}
								else
									usage();
						    }
						}
						else if (args.length == 3)
						{
							String arg0 = args[1];
							String arg1 = args[2];
							
							if (arg0.charAt(0) != '/')
							    System.out.println("Path must start with /");
							else
							{
								if (cmd.equals("begin"))
								{
									beginTrans(arg0, arg1);
								}
								else
									usage();
							}
						}
						
						bash();
					}
			    }		    
				catch (Exception e)
			    {
					e.printStackTrace();
					bash();
			    } 				
			}
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
    
    private void beginTrans(String grupPath, String query)
	{
		try
		{
			TransactionGroup g = null;
			
			for (TransactionGroup tg : groupList)
			{
				if (tg.getGroupPath().equals(grupPath))
				{
					g = tg;
					break;
				}
			}
			
			if (g == null)
				System.out.println("You aren't a member of '" + grupPath + "'.");
			else
			{
				ITransaction t = g.BeginTransaction(query, null);
				
				System.out.print("Waiting result from transaction (query: '"+query+"'): ");
				
				if (t.getResult())
					System.out.println(" COMMITED");
				else
					System.out.println(" ABORTED");
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
	}

	private void bash()
    {
    	String myID = String.valueOf(zk.getSessionId());
    	System.out.print("[client-" + myID + "@zookeper]$ ");
    }
	
	private void usage()
    {
    	System.out.println("Usage:");
		System.out.println("join /groupPath - join group");
		System.out.println("begin /groupPath query - join group");
		System.out.println("leave /groupPath - leave group");
		System.out.println("rm /path - recursively remove /path");
		System.out.println("ls /path - show znode list on 'path'");
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
	
	private void join(String arg)
	{
		try
		{
			TransactionGroup tg = TransactionGroup.joinGroup(arg, zk, new TransactionHandler() );
			
			groupList.add(tg);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	private void leave(String arg)
	{
		try
		{
			TransactionGroup g = null;
			
			for (TransactionGroup tg : groupList)
			{
				if (tg.getGroupPath().equals(arg))
				{
					g = tg;
					break;
				}
			}
			
			if (g == null)
				System.out.println("You aren't a member of '" + arg + "'.");
			else
			{
				g.leaveGroup();
				groupList.remove(g);
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	private void rm(String path)
	{
		try
		{
			List<String> children = zk.getChildren(path, false);

			String cpath;
			
			if (path.endsWith("/"))
				cpath = path;
			else
				cpath = path + "/";
			
			for (String c : children)
			{
				// rm children
				rm(cpath + c);
			}
			
			zk.delete(path, -1);
		} 
		catch (KeeperException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		catch (InterruptedException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
