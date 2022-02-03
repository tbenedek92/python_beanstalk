import beanstalkc

beanstalk = beanstalkc.Connection(host='127.0.0.1', port=11300)
print beanstalk.tubes()