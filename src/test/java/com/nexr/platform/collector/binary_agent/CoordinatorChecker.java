package com.nexr.platform.collector.binary_agent;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.nexr.distributed.ConfigurationException;
import com.nexr.distributed.Coordinator;
import com.nexr.distributed.CoordinatorService;
import com.nexr.distributed.Service;
import com.nexr.distributed.ServiceFactory;
import com.nexr.distributed.ServiceUnavailableException;
import com.nexr.distributed.group.GroupMembership;
import com.nexr.distributed.group.MemberAlreadyExistsException;
import com.nexr.distributed.group.MemberInfo;
import com.nexr.distributed.group.MembershipListener;
import com.nexr.distributed.store.Store;

public class CoordinatorChecker {

	Store myConfigStore;

	public static void main(String args[]) {
		CoordinatorChecker cc = new CoordinatorChecker();
		cc.register();

		cc.startChecker();
	}

	public void startChecker() {
		CheckThread ct = new CheckThread();
		ct.start();
	}

	public void register() {
		Properties props = new Properties();
		props.setProperty("com.nexr.zk.servers", "localhost:2181");

		Coordinator coordinator = null;
		Coordinator configuStoreCoordinator = null;

		try {
			CoordinatorService service = (CoordinatorService) ServiceFactory
					.createService(Service.Type.Coordinator, props);
			service.start();

			CoordinatorService configStoreService = (CoordinatorService) ServiceFactory
					.createService(Service.Type.Coordinator, props);
			configStoreService.start();

			coordinator = service.createCoordinator("NexR");
			configuStoreCoordinator = configStoreService.createCoordinator("NexR");

		} catch (ConfigurationException e) {
			e.printStackTrace();
		} catch (ServiceUnavailableException e) {
			e.printStackTrace();
		}
		GroupMembership myGMS = coordinator.createGroupMembership("MyGMS");

		myConfigStore = configuStoreCoordinator.createStore("MyConfigStore");

		myGMS.leaveForcely("master");
		try {
			myGMS.join("master", "Start");
		} catch (MemberAlreadyExistsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		myGMS.register(new CheckListener());

	}

	Map<String, Object> memberMap = new HashMap<String, Object>();

	class CheckListener implements MembershipListener {

		public void onMemberJoined(List<MemberInfo> members) {
			// TODO Auto-generated method stub
			System.out.println("On Member Join");
			for (MemberInfo member : members) {
				System.out.println(member.getName() + " : " + member.getProfile());
				memberMap.put(member.getName(), member.getProfile());
				myConfigStore.put(memberMap);
			}
		}

		public void onMemberLeft(List<MemberInfo> members) {
			// TODO Auto-generated method stub
			System.out.println("On Member Left");
			for (MemberInfo member : members) {
				System.out.println(member.getName() + " : " + member.getProfile());
				if (myConfigStore.get().get(member.getName().toString()) != "Stop"
						|| myConfigStore.get().get(member.getName().toString()) != "Start") {
					myConfigStore.get().put(member.getName(), "Error");
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
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
}
