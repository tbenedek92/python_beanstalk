import beanstalkc
import time
import json
import yaml

QUEUE_NAME = "action"
output_file = "beanstalk_dump.json"

beanstalk = beanstalkc.Connection(host='12>7.0.0.1', port=11300)
beanstalk.use(QUEUE_NAME)
beanstalk.watch(QUEUE_NAME)
JOBS = []

def get_beanstalk_job_age(status):
    if status == 'ready':
        #job = beanstalk.peek_ready()
        job = beanstalk.reserve(10)
    elif status == 'delayed':
        job = beanstalk.peek_delayed()
    elif status == 'buried':
        job = beanstalk.peek_buried()

    if job:
        JOBS.append(job)
        return job.body, job.stats()['age']
    else:
        return("No job found")


def job_iteration(max_iteration, status, file_name):

    id = 1
    oldest = {'id': 0,
              'age': 0}
    while id <= max_iteration:
        body, age = get_beanstalk_job_age(status)
        if age > oldest['age']:
            oldest['id'] = body
            oldest['age'] = age
        print("Job age: " + str(age) + " body: " + str(body))
        file_name.write(str(body) + "\n")
        id += 1
    print(oldest)
    file_name.write("\n\n Oldest job: " + str(oldest) + "\n")
    return oldest

if __name__ == '__main__':

    # beanstalk.ignore('default')

    num_jobs = {
       # 'delayed': beanstalk.stats_tube(QUEUE_NAME)['current-jobs-delayed'],
       # 'buried': beanstalk.stats_tube(QUEUE_NAME)['current-jobs-buried'],
        'ready': beanstalk.stats_tube(QUEUE_NAME)['current-jobs-ready'],

    }

    for status, queue_length in num_jobs.items():
        f = open(str(status)+'output_file', "w")
        oldest = job_iteration(queue_length, status, f)

        print("Current jobs " + str(status) + ": " + str(queue_length))
        print("Full stats:")
        print(beanstalk.stats_tube(QUEUE_NAME))
        f.close()
    for job in JOBS:
        job.release()
