# oso_hibernation

An Ansible role to deploy the OpenShift Online Hibernation application.

The Hibernation app is responsible for monitoring resource usage for each
project.  The sleeper controller calculates quotas and will place a project in
sleep mode for a configured amount of time over a configured time period.  The
idler controller will monitor each project's cumulative network activity received
and if it's below a configured threshold, a project's services will be idled.

The OpenShift template in this role will deploy the application by building from
the source in the online-hibernation git repository.

This role expects to run with system:admin credentials as it creates roles with
certain elevated privileges to manage the cluster. Typically this is
accomplished by running as root on a master host.

## Dependencies

- lib_openshift role from openshift-ansible must be loaded in a playbook prior to running this role.
- oc_start_build_check role from online-archivist repository. (included via gogitit)

## Role Variables

See defaults/main.yml and templates/hibernation-template.yaml.j2 for default values.

### Required

