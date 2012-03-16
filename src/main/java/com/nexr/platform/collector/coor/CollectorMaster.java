package com.nexr.platform.collector.coor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.nexr.distributed.ConfigurationException;
import com.nexr.distributed.Coordinator;
import com.nexr.distributed.CoordinatorService;
import com.nexr.distributed.Service;
import com.nexr.distributed.ServiceFactory;
import com.nexr.distributed.ServiceUnavailableException;
import com.nexr.distributed.group.GroupMembership;
import com.nexr.distributed.group.Member;
import com.nexr.distributed.group.MemberAlreadyExistsException;
import com.nexr.distributed.group.MemberInfo;
import com.nexr.distributed.group.MembershipListener;
import com.nexr.distributed.store.Store;

public class CollectorMaster {
	private static Logger log = Logger.getLogger(CollectorMaster.class);
	String name;
	String type;
	
	String coordinatorName = "Nexr";
	String groupMembershipName = "MyGMS";
	Store myConfigStore;
	
	Coordinator membershipCoordinator = null;
	Member member = null;
	GroupMembership myGMS = null;
	
	Coordinator configuStoreCoordinator = null;
	
	private static final String START = "START";
	private static final String STOP = "STOP";
	private static final String WARNING = "WARNING";
	
	HashMap<String, Object> config;
	
	public void start(String name, String type) {
		log.info("name " + name + " type " + type);
		this.name = name;
		this.type = type;
		
		Properties props = new Properties();
		props.setProperty("com.nexr.zk.servers", "wolf110:30101,wolf111:30101,wolf112:30101");
		
		
		try {
			CoordinatorService membershipService = (CoordinatorService) ServiceFactory
					.createService(Service.Type.Coordinator, props);
			membershipService.start();
			membershipCoordinator = membershipService.createCoordinator("NexR");
			
			CoordinatorService configStoreService = (CoordinatorService) ServiceFactory
					.createService(Service.Type.Coordinator, props);
			configStoreService.start();
			configuStoreCoordinator = configStoreService.createCoordinator("NexR");
			
			myConfigStore = configuStoreCoordinator.createStore("MyConfigStore");
			
		} catch (ConfigurationException e) {
			e.printStackTrace();
		} catch (ServiceUnavailableException e) {
			e.printStackTrace();
		}
		
		myGMS = membershipCoordinator
				.createGroupMembership("MyGMS");
		
		myGMS.leaveForcely(name);
		try {
			member = myGMS.join(name, type);
			if(myConfigStore.get() != null) {
				config = (HashMap<String, Object>) myConfigStore.get();
			} else {
				config = new HashMap<String, Object>();
			}
			config.put(name, START);
			myConfigStore.put(config);
			
			myGMS.register(new CheckListener());
			
			
		} catch (MemberAlreadyExistsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		CheckThread ct = new CheckThread();
		ct.start();
	}
	
	public void stop() {
		myConfigStore.put((Map<String, Object>) myConfigStore.get().put(name, STOP));
		member.leave();
	}
	
	public void warning() {
		myConfigStore.put((Map<String, Object>) myConfigStore.get().put(name, WARNING));
	}
	
	class CheckListener implements MembershipListener {

		public void onMemberJoined(List<MemberInfo> members) {
			// TODO Auto-generated method stub
			log.info("On Member Join");
			for (MemberInfo member : members) {
				log.info(member.getName() + " : " + member.getProfile());
//				config.put(member.getName(), member.getProfile());
//				myConfigStore.put(config);
			}
		}

		public void onMemberLeft(List<MemberInfo> members) {
			// TODO Auto-generated method stub
			log.info("On Member Left");
			for (MemberInfo member : members) {
				String status = myConfigStore.get().get(member.getName()).toString();
				
				log.info(member.getName() + " : " + member.getProfile() + " Status "  + status);
				if (status.equals("START")) {
					myConfigStore.get().put(member.getName(), "ERROR");
				} 
			}
		}
	}

	class CheckThread extends Thread {
		public CheckThread() {
			// TODO Auto-generated constructor stub
			super("CheckThread");
		}

		public void run() {
			while (true) {
				try {
					HashMap<String, Object> config = (HashMap<String, Object>) myConfigStore.get();
					
					
					List<MemberInfo> memberInfos = myGMS.getMembers(true);
					for(MemberInfo member: memberInfos) {
						if(config.containsKey(member.getName())) {
							log.info(member.getName() + " : " + config.get(member.getName()));
						}
					}
					
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

}
