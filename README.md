# kafka-reassign-tool
A helper script for Kafka to make it easier changing replicas for existing topics

`kafka-reassign-tool` uses Kafka's standard `kafka-reassign-partitions.sh` script and generates data for it.
The main purpose of this script is to make changing replication factor for existing topics easier.
`kafka-reassign-tool` simply generates partition reassignment JSON file that can be fed into `kafka-reassign-partitions.sh`
for execution.

What it can do:
* increase replication for existing topics
* decrease replication factor for existing topics
* remove all replicas from a particular broker so it can be decomissioned
* balance leaders

In all these cases kafka-reassign-tool will try to miminise number of changes which makes it 
different from standard Kafka tools which may generate a better distribution but they may generate a lot of movement
by reallocating each partition to a completely different set of brokers (which may be ok for small installation but
become an issue when you have lots of partitions and brokers).

Note: I wrote this tool before discovering Linkedin's [kafka-tools](https://github.com/linkedin/kafka-tools) for example. I am still using my tool now but you may have a better shot with these more mature ones.

## Configuration
`kafka-reassign-tool` needs to know where Kafka standard scripts are located as well as Zookeeper URL.
If your Kafka is installed in `/opt/kafka` you do not need to supply any additional command line options. Otherwise you
need to provide `--kafka-home <dir>` command line option.
Either way the script will attempt to read zookeeper URL from `config/server.properties` of Kafka home directory.
If that does not work for any reason, you may need to add `--zookeeper <url>` option.

## Changing replication factor
Running the tool with `--replication-factor 3` for example will change partition assignment map so that each partition has 3 replicas in the end.
`kafka-reassign-tool` goes over all partitions one by one and:
* if partition has fewer replicas than needed - adds more replicas by selecting least used brokers (that have fewer replicas for the topic than others)
* if partition has more replicas than needed - removes extra replicas by removing most used brokers (that have more replicas for the topic than others)
* if partition already at target replication factor, the tool still may re-order its replicas to make sure leader distribution is more level among brokers

This is an example output of the tool when it is asked to increase replication factor for a topic with 2 replicas to 3:
```
$ kafka-reassign-tool --topic mytopic --replication-factor 3

Reading /opt/kafka/config/server.properties
Using zookeeper URL: localhost:2181/kafka
Reading list of brokers...
Reading list of topics...
------------------------
Brokers:
  1001
  1002
  1003
  1004
Topics:
  mytopic
------------------------
Getting current assignments...
Building new assignments...
  mytopic-0 : [1001, 1002] => [1001, 1002, 1003]
  mytopic-2 : [1002, 1004] => [1002, 1004, 1001]
  mytopic-1 : [1002, 1004] => [1002, 1003, 1004]
  mytopic-3 : [1003, 1001] => [1003, 1001, 1004]
Saving new assignments into new-assignments.json...
Done
```
And a similar ouptut when it is asked to decrease replication factor for a topic with 2 replicas to 1:
```
$ kafka-reassign-tool --topic mytopic --replication-factor 1

Reading /opt/kafka/config/server.properties
Using zookeeper URL: localhost:2181/kafka
Reading list of brokers...
Reading list of topics...
------------------------
Brokers:
  1001
  1002
  1003
  1004
Topics:
  mytopic
------------------------
Getting current assignments...
Building new assignments...
  mytopic-0 : [1001, 1002] => [1001]
  mytopic-2 : [1002, 1004] => [1002]
  mytopic-1 : [1002, 1004] => [1004]
  mytopic-3 : [1003, 1001] => [1003]
Saving new assignments into new-assignments.json...
Done
```

Of course, it always makes sense to eyeball what changes `kafka-reassign-tool` plans before actually executing them.

### Bulk change
You can supply `--topic` option multiple times:
```
$ kafka-reassign-tool --topic mytopic1 --topic mytopic2 --replication-factor 3
```
or you can omit it completely in which case the same replication factor will be applied to all topics in your cluster
```
$ kafka-reassign-tool --replication-factor 3
```
Which, of course, only makes sense when all your topics share the same replication factor

## Decomissioning a broker
The `--brokers` command line option allows you to specify which brokers can be used for assignment.
`kafka-reassign-tool` remove all assignments from brokers that are not in that list (replacing it with others to maintain replication factor).
This can be used to decomission a certain broker by removing all replicas from it.

For example, the command below does not list broker 1004 so if any partition used it as a replica, it will be changed to something else:
```
$ kafka-reassign-tool --topic mytopic --replication-factor 2 --brokers 1001,1002,1003

Reading /opt/kafka/config/server.properties
Using zookeeper URL: localhost:2181/kafka
Reading list of brokers...
Reading list of topics...
------------------------
Brokers:
  1001
  1002
  1003
  1004
Topics:
  mytopic
------------------------
Getting current assignments...
Building new assignments...
  mytopic-0 : [1001, 1002] => [1001, 1002]
  mytopic-2 : [1002, 1004] => [1002, 1003]
  mytopic-1 : [1002, 1004] => [1002, 1001]
  mytopic-3 : [1003, 1001] => [1003, 1001]
Saving new assignments into new-assignments.json...
Done
```

Note that this operation can also be used in bulk by providing multiple `--topic` options or omitting it completely to select all topics.
However, keep in mind that the same value from `--replication-factor` will be used for all selected topics.

## Applying the change
After successful invocation, `kafka-reassign-tool` generates `new-assignments.json` file which can be then applied as
```
kafka-reassign-partitions.sh --zookeeper <url> --reassignment-json-file new-assignments.json --execute --throttle 100000000
```
the example above throttles replication to 100Mb/sec. You may decide to use a different limit or to omit it completely.
If throttling was used, at the end you must verify reassignmnet with
```
kafka-reassign-partitions.sh --zookeeper <url> --reassignment-json-file new-assignments.json --verify
```
so replication quota is reset. For more details see https://kafka.apache.org/documentation.html#rep-throttle
