# Online force-sleep controller

Controller for monitoring and restricting resource usage of free tier accounts. Listens to events in a cluster and caches data on resources by namespace (project).

The `PERIOD` is a rolling timeframe during which pods' resource usage is considered.

`QUOTA_HOURS` refers to the maximum number of quota-hours usable within the `PERIOD` before the resource (or project) is put into force-sleep mode. A quota-hour is defined for pods as a pod using its full memory quota for one hour.

Once a project has exceeded the `QUOTA_HOURS` limit, that project's scalable resources are scaled to 0 replicas.  Pods' quota-hour usage within a project accumulate during the rolling `PERIOD` until `QUOTA_HOURS` is met. In that case, a `force-sleep` quota is placed on the project with a hard limit on `pods=0`. This quota persists for `PROJECT_SLEEP_LENGTH`.  Upon removal of the `force-sleep` quota, services are placed in an idled state.  Project deployments will be scaled up to the pre-sleep value when the service within that project receives network traffic, using the same logic as oc idle and the origin unidling controller.

Every `SLEEP_SYNC_PERIOD`, the cached data on each project will be queried and the projects' quota-hour usage will be calculated and, if necessary, force-sleep will be added to (or removed from) the project.

Every `IDLE_SYNC_PERIOD`, prometheus metrics will be queried to get the cumulative network traffic received for all pods in a project over the `IDLE_QUERY_PERIOD`.  If network traffic recieved is below a configured threshold, services in the project will be idled. Also, replication controllers, replicasets, deployments and deployment configs are scaled to 0 and all pods are deleted. Upon receiving network traffic, scalable resources within idled projects are scaled to whatever the value was in the RC/RS/Deployment/DC before being idled. The auto-idler uses the same logic as oc idle and the origin unidling controller.

The auto-idler queries prometheus.  Therefore, prometheus must be deployed in the cluster to run the auto-idling controller.

```
Note: Prometheus has a default collection interval of 1 minute.  A query has to be at least 2 times
      that interval.  Therefore, in testing this component, the IDLE_QUERY_PERIOD should never be
      set to less than 2 minutes.  Prometheus will not return any projects as below idling threshold
      if the query period is less than 2 minutes.
```

Usage - deploy in cluster with the following:
```
oc create -f template.yaml -n openshift-infra
oc process -n openshift-infra hibernation | oc apply -n openshift-infra -f -
```

`glog` levels generally follow this structure:
* 3: Resource/watch event level messages
* 2: Project/sleep/idle level messages
* 1: Sleeper/Idler/cluster level messages

