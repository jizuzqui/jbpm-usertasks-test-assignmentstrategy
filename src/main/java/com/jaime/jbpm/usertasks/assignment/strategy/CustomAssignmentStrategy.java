package com.jaime.jbpm.usertasks.assignment.strategy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.jbpm.services.task.assignment.LoadCalculator;
import org.jbpm.services.task.assignment.UserTaskLoad;
import org.kie.api.KieServices;
import org.kie.api.builder.KieScanner;
import org.kie.api.runtime.ClassObjectFilter;
import org.kie.api.runtime.EnvironmentName;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieRuntimeFactory;
import org.kie.api.runtime.KieSession;
import org.kie.api.task.TaskContext;
import org.kie.api.task.UserInfo;
import org.kie.api.task.model.Group;
import org.kie.api.task.model.OrganizationalEntity;
import org.kie.api.task.model.Task;
import org.kie.api.task.model.User;
import org.kie.dmn.api.core.DMNContext;
import org.kie.dmn.api.core.DMNModel;
import org.kie.dmn.api.core.DMNResult;
import org.kie.dmn.api.core.DMNRuntime;
import org.kie.dmn.core.internal.utils.DynamicDMNContextBuilder;
import org.kie.internal.task.api.assignment.Assignment;
import org.kie.internal.task.api.assignment.AssignmentStrategy;
import org.kie.internal.task.api.model.InternalPeopleAssignments;
import org.kie.server.services.dmn.modelspecific.DMNFEELComparablePeriodSerializer;
import org.kie.server.services.dmn.modelspecific.KogitoDMNResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * To activate this assignment strategy, two system properties have to be set at server level 
 * (jBPM also allows to set strategy at User Task level by setting an input parameter in the UT):
 * 
 *  - org.jbpm.task.assignment.enabled=true
 *  - org.jbpm.task.assignment.strategy=Custom (Custom is the identifier that has been set in the custom implementation)
 */
public class CustomAssignmentStrategy implements AssignmentStrategy {

	private static final String IDENTIFIER = "Custom";
	private static final Logger logger = LoggerFactory.getLogger(CustomAssignmentStrategy.class);

	private KieServices kieServices = KieServices.Factory.get();
	private KieContainer droolsKieContainer;
	private KieContainer dmnKieContainer;
	private KieScanner droolsKieScanner;
	private KieScanner dmnKieScanner;
	private LoadCalculator calculator;
	private DMNRuntime dmnRuntime;
	private boolean skillsFeatureActive = false;
	
	private static final com.fasterxml.jackson.databind.ObjectMapper objectMapper = new com.fasterxml.jackson.databind.ObjectMapper()
            .registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule())
            .registerModule(new com.fasterxml.jackson.databind.module.SimpleModule()
                            .addSerializer(org.kie.dmn.feel.lang.types.impl.ComparablePeriod.class,
                                           new DMNFEELComparablePeriodSerializer()))
            .disable(com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .disable(com.fasterxml.jackson.databind.SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS);

	public CustomAssignmentStrategy() throws InstantiationException, IllegalAccessException, ClassNotFoundException {

		String droolsReleaseId = System.getProperty("usertasks.assignment.strategy.custom.drools.releaseid");
		String droolsScannerInterval = System.getProperty("usertasks.assignment.strategy.custom.drools.scannerinterval");
		
		String dmnReleaseId = System.getProperty("usertasks.assignment.strategy.custom.dmn.releaseid");
		String dmnScannerInterval = System.getProperty("usertasks.assignment.strategy.custom.dmn.scannerinterval");

		String[] droolsGav = droolsReleaseId.split(":");
		logger.debug("Creating Drools KieContainer for {} to be used for task assignments", droolsReleaseId);
		this.droolsKieContainer = kieServices.newKieContainer(kieServices.newReleaseId(droolsGav[0], droolsGav[1], droolsGav[2]));
		
		if (droolsScannerInterval != null) {
			Long pollingInterval = Long.parseLong(droolsScannerInterval);
			logger.debug("Scanner to be enabled for {} container with polling interval set to {}", droolsKieContainer, pollingInterval);

			this.droolsKieScanner = this.kieServices.newKieScanner(droolsKieContainer);
			this.droolsKieScanner.start(pollingInterval);
			logger.debug("Scanner for container {} started at {}", droolsKieContainer, new Date());
		}

		String calculatorClass = System.getProperty(
				"usertasks.assignment.strategy.custom.drools.filters.workload.calculator",
				"org.jbpm.services.task.assignment.impl.TaskCountLoadCalculator");

		calculator = (LoadCalculator)Class.forName(calculatorClass).newInstance();
		
		if(dmnReleaseId != null && !dmnReleaseId.isEmpty()) {
			skillsFeatureActive = true;
			
			String[] dmnGav = dmnReleaseId.split(":");
			this.dmnKieContainer = kieServices.newKieContainer(kieServices.newReleaseId(dmnGav[0], dmnGav[1], dmnGav[2]));
			
			if (dmnScannerInterval != null) {
				Long dmnPollingInterval = Long.parseLong(dmnScannerInterval);
				logger.debug("Scanner to be enabled for {} container with polling interval set to {}", dmnKieContainer, dmnPollingInterval);
	
				this.dmnKieScanner = this.kieServices.newKieScanner(dmnKieContainer);
				this.dmnKieScanner.start(dmnPollingInterval);
				logger.debug("Scanner for container {} started at {}", dmnKieContainer, new Date());
			}
			
			this.dmnRuntime = KieRuntimeFactory.of(dmnKieContainer.getKieBase()).get(DMNRuntime.class);
		}
	}

	@Override
	public Assignment apply(Task task, TaskContext taskContext, String excludedUser) {

		taskContext.loadTaskVariables(task);
		Map<String, Object> taskContextVariables = task.getTaskData().getTaskInputVariables();
		List<String> finalUsersList = new ArrayList<String>();
		
		// Step 1: retrieve authorized users by role (IAM, Database... whatever)
		List<User> usersByRoleListTemp = this.filterUsersByRole(task, taskContext, excludedUser);
		List<String> usersByRoleList = new ArrayList<String>();
		
		for (User user : usersByRoleListTemp) {
			usersByRoleList.add(user.getId());
		}
		
		if(skillsFeatureActive) {
			// Step 2 (optional): retrieve the skill required for the user task (Drools Domain-specific business rules)
			// I've used the implementation in this link: https://github.com/jizuzqui/UserTaskAssignmentDRL
			String requiredSkill = this.filterSkillsByTaskContext(task, taskContext);
			
			// Step 3 (optional): retrieve users with the required skills (Domain-specific DMN)
			// I've used the following DMN: https://github.com/jizuzqui/SkillsMatrix
			List<String> usersBySkillList = this.filterUsersBySkill(requiredSkill);
						
			// Step 4: match authorized users with skilled users
			finalUsersList = usersByRoleList.stream()
					  .distinct()
					  .filter(usersBySkillList::contains)
					  .collect(Collectors.toList());
		}
		else {
			finalUsersList = usersByRoleList;
		}
		
		
		String filterAssignmentByCurrentWorkloadActivated = System.getProperty("usertasks.assignment.strategy.custom.filters.workload");
		String inputParam = (String)taskContextVariables.get("workloadStrategyActivated");

		String selected = null;
		
		// Optionally, filter the less busy user making use of the Load Calculator (jBPM provides a default one based on the number of tasks a user has in the Task table of jbpm schema)
		if((filterAssignmentByCurrentWorkloadActivated != null && !filterAssignmentByCurrentWorkloadActivated.isEmpty()
				&& Boolean.valueOf(filterAssignmentByCurrentWorkloadActivated))
				|| (inputParam != null && !inputParam.isEmpty() && Boolean.valueOf(inputParam))) {
			// Step 5 (optional): select most free user (LoadBalanceCalculator)		
			String selectedCandidate = this.filterAssignmentByWorkload(finalUsersList, task, taskContext, excludedUser);

			if(selectedCandidate != null)
				selected = selectedCandidate;            	
		}

		if(selected == null) {
			selected = finalUsersList.get(0);
		}

		logger.debug("Selected assignment is {} for task {}", selected, task);
		return new Assignment(selected);
	}

	private String filterSkillsByTaskContext(Task task, TaskContext taskContext) {
		logger.debug("Using rules to assign actual owner to task {}", task);
		KieSession kieSession = this.droolsKieContainer.newKieSession();
		Set<Assignment> assignments = new TreeSet<>();
		String skill = null;
		
		try {
			Map<String, Object> taskInputs = task.getTaskData().getTaskInputVariables();
			taskInputs.put("taskName", task.getName());
			
			taskInputs.keySet().forEach((k -> System.out.println((k + ":" + taskInputs.get(k)))));
			
			kieSession.insert(taskInputs);			
			kieSession.fireAllRules();
			
			// Collection<Assignment> results = (Collection<Assignment>) kieSession.getObjects(new ClassObjectFilter(Assignment.class));
			Collection<String> results = (Collection<String>) kieSession.getObjects(new ClassObjectFilter(String.class));

			if(results.isEmpty()) {
				return null;
			}
			
			// assignments.addAll(results);
			
			logger.debug("Rule evaluation completed with selected assignments of {}", assignments);
			
//			if (assignments.isEmpty()) {
//				logger.debug("No assignments found by BusinessRule strategy");
//				return null;
//			}

		} finally {
			kieSession.dispose();
			logger.debug("KieSession in BusinessRule disposed");
		}
		
		
		return skill;
		// return assignments;
	}

	private List<User> filterUsersByRole(Task task, TaskContext taskContext, String excludedUser) {
		List<User> userList = this.getUsersFromGroups(task, taskContext, excludedUser);
		
		return userList;
	}
	
	private String filterAssignmentByWorkload(List<String> candidates, Task task, TaskContext taskContext, String excludedUser) {
		
		List<User> userList = new ArrayList<User>();
				
		Collection<UserTaskLoad> loads = calculator.getUserTaskLoads(userList, taskContext);
		UserTaskLoad lightestLoad = loads.stream().min(UserTaskLoad::compareTo).orElse(null);

		return lightestLoad != null ? lightestLoad.getUser().getId():null;
	}

	private List<String> filterUsersBySkill(String skillId) {
		
		List<String> users = new ArrayList<String>();
		String dmnModelName = System.getProperty("usertasks.assignment.strategy.custom.dmn.model.name", "SkillUsers");
		
		 try {			 
			 List<DMNModel> modelsWithID = dmnRuntime.getModels().stream().filter(m -> m.getName().equals(dmnModelName)).collect(Collectors.toList());
	            if (modelsWithID.isEmpty()) {
	                throw new RuntimeException("No DMN model with ID " + dmnModelName + " found");
	            }
	            
	            DMNModel dmnModel = modelsWithID.get(0);
	            
	            Map<String, Object> dmnInputs = new HashMap<String, Object>();
	            dmnInputs.put("Skill Input", skillId);
	            DMNContext dmnContext = new DynamicDMNContextBuilder(dmnRuntime.newContext(), dmnModel).populateContextWith(dmnInputs);

	            DMNResult determinedResult = dmnRuntime.evaluateAll(dmnModel, dmnContext);
	            
	            users = (List<String>)determinedResult.getDecisionResultByName("Get Users by Skill").getResult();
	            
	        } catch (Exception e) {
	            logger.error("Error when evaluating decision model with ID " + dmnModelName);
	        }
		 
		 return users;
	}
	
	private static List<OrganizationalEntity> getExcludedEntities(Task task, UserInfo userInfo) {
		List<OrganizationalEntity> excluded = ((InternalPeopleAssignments) task.getPeopleAssignments()).getExcludedOwners();

		List<OrganizationalEntity> excludedUsers = new ArrayList<>();
		for (OrganizationalEntity entity : excluded) {
			if (entity instanceof Group) {
				userInfo.getMembersForGroup((Group) entity).forEachRemaining(excludedUsers::add);
			}
		}
		excluded.addAll(excludedUsers);

		return excluded;
	}

	private Function<OrganizationalEntity, User> entityToUser = (oe) -> {
		return (User)oe;
	};

	private List<User> getUsersFromGroups(Task task, TaskContext taskContext, String excludedUser) {
		/*
		 * To be able to resolve users of a specific group, jbpm requires a UserInfo implementation.
		 * 
		 * I've made use of the DBUserInfoImpl that jBPM provides out of the box. To use it:
		 * 
		 *  - Set the following system properties to activate the user info implementation:
		 *      + org.jbpm.ht.userinfo=db
		 *      + jbpm.user.info.properties=file:///opt/jboss/repositories/jbpm.user.info.properties (or any other path you want to use).
		 *  - Create the jbpm.user.info.properties file to provide the required database queries
		 *    
		 *    Example:
		 *
		 *		# data source JNDI name to be used - defaults to same as KIE server is using
		 *		db.ds.jndi.name=java:jboss/datasources/jbpmRuntime-MySQL-DS
		 *		
		 *		# db query for finding name of user by its id
		 *		db.name.query=SELECT name FROM bpmusers WHERE id = ?;
		 *		
		 *		# db query for finding email of user by its id
		 *		db.email.query=select mail from bpmusers where id = ?
		 *		
		 *		# db query for finding preferred language of user by its id
		 *		db.lang.query=select 'ES' from bpmusers where id = ?
		 *		
		 *		# db query for users that belong to given group
		 *		db.group.mem.query=select user_id from bpmusergroups where group_id = ?
		 */
		UserInfo userInfo = (UserInfo) ((org.jbpm.services.task.commands.TaskContext)taskContext).get(EnvironmentName.TASK_USER_INFO);
		List<OrganizationalEntity> excluded = (getExcludedEntities(task, userInfo));

		// Get the the users from the task's the potential owners, making sure that excluded users are not included
		List<OrganizationalEntity> potentialOwners = task.getPeopleAssignments().getPotentialOwners().stream()
				.filter(oe -> oe instanceof User && !excluded.contains(oe) && !oe.getId().equals(excludedUser))
				.collect(Collectors.toList());

		// Get the users belonging to groups that are potential owners
		task.getPeopleAssignments().getPotentialOwners().stream().filter(oe -> oe instanceof Group)
		.forEach(oe -> {
			Iterator<OrganizationalEntity> groupUsers = userInfo.getMembersForGroup((Group)oe);
			
			if (groupUsers != null) {
				groupUsers.forEachRemaining(user -> {
					if (user != null && !excluded.contains(user) && !potentialOwners.contains(user) && !user.getId().equals(excludedUser)) {
						potentialOwners.add(user);
					}
				});
			}
		});

		logger.debug("Asking the load calculator [{}] for task loads for the users {}",calculator.getIdentifier(),potentialOwners);

		List<User> users = potentialOwners.stream().map(entityToUser).collect(Collectors.toList());
		
		return users;
	}
	
	@Override
	public String getIdentifier() {
		return IDENTIFIER;
	}
}
