AWS SQS River Plugin for ElasticSearch
==================================

The AWS SQS plugin uses Amazon's SQS as a river by long polling for messages from a given queue. Right after a message is indexed it gets deleted from the queue.

Messages are in the following JSON format:

    {
      "_id": "123",
      "_index": "es_index_name",
      "_type": "es_data_type",
      "_data": { "key1": "value1" ...}
    }

If `_data` is missing the data with this id will be deleted from the index.

If `_index` is missing it will fallback to the index that was initially configured, otherwise the `_index` property overrides the default configuration and allows you to dynamically switch between indexes.

The fields `_id` and `_type` are required.

When the queue is empty the river will sleep for `sleep` seconds before sending a new request for messages to the queue.
Long polling is done by the Amazon SQS client using the `waitTimeSeconds` attribute which is set to `longpolling_interval` _(must be between 0 and 20)_.

To configure put this in your `elasticsearch.yml`:

    cloud.aws.region: AWS REGION
    cloud.aws.access_key: AWS ACCESS KEY
    cloud.aws.secret_key: AWS SECRET KEY
    cloud.aws.sqs.queue_url: AWS QUEUE URL
    cloud.aws.sqs.debug: (false by default)
    cloud.aws.sqs.sleep: (seconds)
    cloud.aws.sqs.longpolling_interval: (seconds)

Or use a river configuration like this:

    curl -XPUT 'localhost:9200/_river/my_sqs_river/_meta' -d '{
      "type": "amazonsqs",
      "amazonsqs": {
          "region": "AWS REGION",
          "access_key": "AWS ACCESS KEY",
          "secret_key": "AWS SECRET KEY",
          "queue_url": "AWS QUEUE URL",
          "debug": false,
          "sleep": 60,
          "longpolling_interval": 20
        },
        "index": {
          "max_messages": 10,
          "index": "es_index_name"
        }
    }'
