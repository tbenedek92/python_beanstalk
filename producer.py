import beanstalkc
import platform

queue_name = "default"
num_of_messages_to_produce = 100

beanstalk = beanstalkc.Connection(host='localhost', port=11300)
beanstalk.use(queue_name)

count = 1
if platform.system()=='Darwin':
    while count <= num_of_messages_to_produce:
        beanstalk.put('{"name":"job' + str(count) + '","value":"test_value' + str(count) + '"}')
        print('Job ' + str(count) + ' sent!')
        count += 1