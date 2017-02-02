#Online force-sleep controller

Controller for monitoring and restricting resource usage of free tier accounts. Listens to pod and replication controller events in a cluster and caches data on these resources by namespace (project).

The `PERIOD` is a rolling timeframe during which pod, replication controller, and deployment config resource usage is considered.

`QUOTA_HOURS` refers to the maximum number of quota-hours usable within the `PERIOD` before the resource (or project) is put into force-sleep mode. A quota-hour is defined for pods as a pod using its full memory quota for one hour. For replication controllers, it is defined as simply having one or more active replicas for one hour. For deployment configs, it is having one or more active deployments for one hour.

Once a replication controller has exceeded the `QUOTA_HOURS` limit, that RC is scaled to 0 replicas. Any RCs active for a deployment config contribute cumulatively toward that DC's quota-hour usage, and once it is exceeded all RCs connected to that DC will be scaled to 0 replicas. As a catch-all to deter users from skirting these rules, pods' quota-hour usage within a project accumulate during the rolling `PERIOD` until `QUOTA_HOURS` is met. In that case, a `force-sleep` quota is placed on the project with a hard limit on `pods=0`. This quota persists for `PROJECT_SLEEP_LENGTH`. 

Every `SYNC_PERIOD`, the cached data on each project will be queried and the projects' quota-hour usage will be calculated and, if necessary, force-sleep will be added to (or removed from) the project.

usage:
```
oc new-app -n openshift-infra \
   -f template.yaml \
   -p QUOTA_HOURS=16h \
   -p PERIOD=24h \
   -p SYNC_PERIOD=1h \
   -p PROJECT_SLEEP_LENGTH=8h \
   -p WORKERS=10 \
   -p EXCLUDE_NAMESPACES="openshift,openshift-infra"
   -p TERMINATING_QUOTA=1Gi \
   -p NONTERMINATING_QUOTA=1Gi
```

Building locally:
`docker build -f Dockerfile.local -t openshift/force-sleep .`

`glog` levels generally follow this structure:
3: Resource/watch event level messages
2: Project/sleep level messages
1: Sleeper/cluster level messages

(WIP) Ansible role contained in `../ansible/roles/force_sleep/tasks/main.yml`