package zk_lock.imp.client;

import java.io.IOException;
import java.util.List;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import zk_lock.imp.Main;

public class Client implements Watcher
{
	private ZooKeeper zk;
	private volatile boolean dead;	
	
	/* properties */
	
	public boolean getDead()
	{
		return dead;
	}

	/* constructor */
	
	public Client(String[] hosts, int port) throws IOException
	{
		StringBuilder connString = new StringBuilder();
		
		for(String h : hosts)
			connString.append(h + ":").append(port).append(",");
		
		connString.setLength(connString.length() - 1);		
		
		zk = new ZooKeeper(connString.toString(), 3000, this);
				
		
		dead = false;
	}
		
	@Override
	public void process(WatchedEvent event)
	{
		System.out.println("evento");
		
		if (event.getType() == Event.EventType.None)
		{
			// estado da conexao mudou
			switch(event.getState())
			{
				case SyncConnected:
					// conectamos
					// podemos iniciar
					run();
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
	private void run()
	{
		try
		{
			List<String> children = zk.getChildren("/", false);
			
			for (String c : children)
				System.out.println(c);
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
