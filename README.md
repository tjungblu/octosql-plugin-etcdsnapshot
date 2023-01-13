# OctoSQL Plugin for ETCD (snapshots)

This is a plugin to run queries against etcd snapshots. This plugin is compatible with the key layout of Kubernetes.
Values are currently not support.

The very basic example is listing all keys:

> "SELECT * from etcd.snapshot"


where "etcd.snapshot" is an etcd snapshot in the current folder that was generated with `etcdctl snapshot save`.


Awesome queries you can run against your etcd snapshots now:

```sql
$ octosql "SELECT COUNT(*) FROM etcd.snapshot"
+-------+
| count |
+-------+
| 10953 |
+-------+
```

```sql
$ octosql "SELECT * FROM etcd.snapshot WHERE name='console'"
+----------------------------------------------------------------------------------+-----------------+-------------------------+------------------------+---------------------+-----------+
|                                       key                                        | apiserverPrefix |        apigroup         |      resourceType      |      namespace      |   name    |
+----------------------------------------------------------------------------------+-----------------+-------------------------+------------------------+---------------------+-----------+
| '/kubernetes.io/clusterrolebindings/console'                                     | 'kubernetes.io' | <null>                  | 'clusterrolebindings'  | <null>              | 'console' |
| '/kubernetes.io/clusterroles/console'                                            | 'kubernetes.io' | <null>                  | 'clusterroles'         | <null>              | 'console' |
| '/kubernetes.io/config.openshift.io/clusteroperators/console'                    | 'kubernetes.io' | <null>                  | 'config.openshift.io'  | 'clusteroperators'  | 'console' |
| '/kubernetes.io/deployments/openshift-console/console'                           | 'kubernetes.io' | <null>                  | 'deployments'          | 'openshift-console' | 'console' |
| '/kubernetes.io/monitoring.coreos.com/servicemonitors/openshift-console/console' | 'kubernetes.io' | 'monitoring.coreos.com' | 'servicemonitors'      | 'openshift-console' | 'console' |
| '/kubernetes.io/poddisruptionbudgets/openshift-console/console'                  | 'kubernetes.io' | <null>                  | 'poddisruptionbudgets' | 'openshift-console' | 'console' |
| '/kubernetes.io/rolebindings/kube-system/console'                                | 'kubernetes.io' | <null>                  | 'rolebindings'         | 'kube-system'       | 'console' |
| '/kubernetes.io/serviceaccounts/openshift-console/console'                       | 'kubernetes.io' | <null>                  | 'serviceaccounts'      | 'openshift-console' | 'console' |
| '/kubernetes.io/services/endpoints/openshift-console/console'                    | 'kubernetes.io' | 'services'              | 'endpoints'            | 'openshift-console' | 'console' |
| '/kubernetes.io/services/specs/openshift-console/console'                        | 'kubernetes.io' | 'services'              | 'specs'                | 'openshift-console' | 'console' |
| '/openshift.io/oauth/clients/console'                                            | 'openshift.io'  | <null>                  | 'oauth'                | 'clients'           | 'console' |
| '/openshift.io/routes/openshift-console/console'                                 | 'openshift.io'  | <null>                  | 'routes'               | 'openshift-console' | 'console' |
+----------------------------------------------------------------------------------+-----------------+-------------------------+------------------------+---------------------+-----------+
```

```sql
$ octosql "SELECT COUNT(*) FROM etcd.snapshot"
+-------+
| count |
+-------+
| 10953 |
+-------+
```

## Installation

