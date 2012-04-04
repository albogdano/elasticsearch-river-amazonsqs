AWS SQS River Plugin for ElasticSearch
==================================

The AWS SQS plugin uses Amazon's SQS as a river by pulling messages from a given queue. Right after a message is indexed it gets deleted from the queue.

Messages are in the format `{"_id": "123", "_index":"es_index_name", "_type": "es_data_type", "_data": {"key1":"value1"...}}`.
If `_data` is missing the data with this id will be deleted from the index.
If `_index` is missing the it will fallback to default index

The fields `_id` and `_type` are required.

To configure put this in your `elasticsearch.yml`:

    cloud.aws.region: region
    cloud.aws.access_key: key
    cloud.aws.secret_key: key
    cloud.aws.sqs.queue_url: url

Or use river configuration:

curl -XPUT 'localhost:9200/_river/my_sqs_river/_meta' -d '{
  "type": "amazonsqs",
  "amazonsqs": {
      "region": "eu-west-1",
      "accesskey": "AWS ACCESS KEY",
      "secretkey": "AWS SECRET KEY",
      "queue_url": "https://eu-west-1.queue.amazonaws.com/123456789/my-queue-url"
    },
    "index": {
      "max_messages": 10,
      "timeout_seconds": 10,
      "index": "se"
    }

}'

In order to install the plugin, simply run: `bin/plugin -install aleski/elasticsearch-river-amazonsqs/1.1`.
