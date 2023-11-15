# OctoSQL Plugin for ETCD

This is a plugin to run queries against etcd snapshots and raw database folders. This plugin is compatible with the key layout of Kubernetes.

*Values are not supported (yet).*

The very basic example is listing all keys:

```sql
$ octosql "SELECT * FROM etcd.snapshot"
```

where "etcd.snapshot" is an etcd snapshot in the current folder that was generated with `etcdctl snapshot save`.

Alternatively, you can supply a direct path:

```sql
$ octosql "SELECT * FROM ./etcdsnapshot/data/basic.snapshot"
```

When you don't have the snapshot extension, you can also directly supply the plugin source via:

```sql
$ octosql "SELECT * FROM etcdsnapshot.etcddump"
```

This is also required when loading from a "dataDir" directly:

```sql
$ octosql "SELECT * FROM etcdsnapshot./var/lib/etcd/"
```

The table schema currently looks like that:

```sql
$ octosql "SELECT * FROM etcd.snapshot" --describe
+-------------------+-----------------+------------+
|       name        |      type       | time_field |
+-------------------+-----------------+------------+
| 'apigroup'        | 'NULL | String' | false      |
| 'apiserverPrefix' | 'NULL | String' | false      |
| 'key'             | 'String'        | false      |
| 'name'            | 'NULL | String' | false      |
| 'namespace'       | 'NULL | String' | false      |
| 'resourceType'    | 'NULL | String' | false      |
| 'valueSize'       | 'Int'           | false      |
+-------------------+-----------------+------------+
```

* `key` is the actual key in etcd, all others can be NULL.
* `apiserverPrefix` is the prefix defined in the apiserver, for example openshift.io, kubernetes.io or registry
* `apigroup` are specified groups, eg. cloudcredential.openshift.io
* `resourceType` are the usual k8s resources like "pod", "service", "deployment"
* `namespace` is the namespace of that resource
* `name` is the resource name
* `valueSize` is the amount of bytes needed to store the value


## Examples

Awesome queries you can run against your etcd snapshots now:

```sql
$ octosql "SELECT COUNT(*) FROM etcd.snapshot"
+-------+
| count |
+-------+
| 10953 |
+-------+
```

### Get all keys that are named "console"

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

### Get how many events are emitted by namespace

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
   ....    
```

### How many image streams are there?

```sql
$ octosql "SELECT COUNT(*) AS CNT FROM etcd.snapshot where resourceType='imagestreams'  ORDER BY CNT DESC"
+-----+
| CNT |
+-----+
|  60 |
+-----+
```

### Diff between two snapshots

```sql
$ octosql "SELECT l.key FROM etcd.snapshot l LEFT JOIN etcd_later.snapshot r ON l.key = r.key WHERE r.key IS NULL"
+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|                                                                                                key                                                                                                |
+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| '/kubernetes.io/certificatesigningrequests/csr-2sxqs'                                                                                                                                             |
| '/kubernetes.io/certificatesigningrequests/csr-2zmsm'                                                                                                                                             |
| '/kubernetes.io/certificatesigningrequests/csr-66szn'                                                                                                                                             |
| '/kubernetes.io/certificatesigningrequests/csr-7zpfn'                                                                                                                                             |
| '/kubernetes.io/certificatesigningrequests/csr-dwg5r'                                                                                                                                             |
| '/kubernetes.io/certificatesigningrequests/csr-fqfcr'                                                                                                                                             |
| '/kubernetes.io/certificatesigningrequests/csr-h6kzp'                                                                                                                                             |
| '/kubernetes.io/certificatesigningrequests/csr-jz5vr'                                                                                                                                             |
| '/kubernetes.io/certificatesigningrequests/csr-n7qzt'                                                                                                                                             |
   ....
```

### What namespaces are taking the most space?

```sql
$ octosql "SELECT namespace, SUM(valueSize) AS S from etcd.snapshot GROUP BY namespace ORDER BY S DESC"

----------------------------------------------------+---------+
|                     namespace                      |    S    |
+----------------------------------------------------+---------+
| 'openshift-monitoring'                             | 3635823 |
| 'customresourcedefinitions'                        | 2752055 |
| 'apirequestcounts'                                 | 2138028 |
| <null>                                             | 1737472 |
| 'openshift-config-managed'                         | 1278729 |
  ....
```

## Installation

1. Follow the instructions on [OctoSQL](https://github.com/cube2222/octosql) to install the query binary.
2. Register the etcdsnapshot with the "snapshot" extension like that:
```
$ mkdir -p ~/.octosql/ && echo "{\"snapshot\": \"etcdsnapshot\"}" > ~/.octosql/file_extension_handlers.json
```
3. Add this repository as a plugin repo:
```
$ octosql plugin repository add https://raw.githubusercontent.com/tjungblu/octosql-plugin-etcdsnapshot/main/plugin_repository.json
```
4. Install the plugin:
```
$ octosql plugin install etcdsnapshot/etcdsnapshot
```

Try it out with a snapshot file named "etcd.snapshot" in the current folder: 
> octosql "SELECT * FROM etcd.snapshot"

### Build locally

In order to get a build directly from the source, you can leverage the makefile to build:

> make build

and install it directly in the plugin directory with:

> make install

