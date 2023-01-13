# OctoSQL Plugin for ETCD (snapshots)

This is a plugin to run queries against etcd snapshots. This plugin is compatible with the key layout of Kubernetes.
Values are currently not support.

The very basic example is listing all keys:

```sql
$ octosql "SELECT * FROM etcd.snapshot"
```

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

Get all keys that are named "console":

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

Get how many events are emitted by namespace:

```sql
$ octosql "SELECT namespace, COUNT(*) AS CNT FROM etcd.snapshot where resourceType='events' GROUP BY namespace ORDER BY CNT DESC"
+----------------------------------------------------+-----+
|                     namespace                      | CNT |
+----------------------------------------------------+-----+
| 'openshift-monitoring'                             | 460 |
| 'openshift-kube-apiserver-operator'                | 371 |
| 'openshift-etcd-operator'                          | 353 |
| 'openshift-multus'                                 | 347 |
| 'openshift-etcd'                                   | 340 |
| 'openshift-cluster-csi-drivers'                    | 285 |
| 'openshift-kube-controller-manager-operator'       | 278 |
| 'openshift-kube-controller-manager'                | 261 |
| 'openshift-apiserver'                              | 234 |
| 'openshift-authentication-operator'                | 227 |
| 'openshift-kube-apiserver'                         | 222 |
...    
```

How many image streams are there?

```sql
$ octosql "SELECT COUNT(*) AS CNT FROM etcd.snapshot where resourceType='imagestreams'  ORDER BY CNT DESC"
+-----+
| CNT |
+-----+
|  60 |
+-----+
```

## Installation

1. Follow the instructions on [OctoSQL](https://github.com/cube2222/octosql) to install the query binary.
2. Register the etcdsnapshot with the "snapshot" extension like that:
> echo "{\"snapshot\": \"etcdsnapshot\"}" > ~/.octosql/file_extension_handlers.json
3. Install the plugin with


Try it out with a snapshot file named "etcd.snapshot" in the current folder: 
> octosql "SELECT * FROM etcd.snapshot"
