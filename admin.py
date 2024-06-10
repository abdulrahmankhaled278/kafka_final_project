from confluent_kafka.admin import AdminClient, NewTopic

conf = { 'bootstrap.servers': '34.68.55.43:9094,34.136.142.41:9094,34.170.19.136:9094'}

ac = AdminClient(conf)

me = 'abdulrahmankhaled'

topic = me

res = ac.create_topics([NewTopic(topic, num_partitions=3)])

res[topic].result()

