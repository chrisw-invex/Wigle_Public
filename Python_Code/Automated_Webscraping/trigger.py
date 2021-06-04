import datetime
import boto3

def trigger_Lambda(schedule):
    current_time = datetime.datetime.now()
    freq = []
    for i in schedule.keys():
        if(current_time.hour in schedule[i]):
            freq.append(i)
    if len(freq) > 0:
        sqs = boto3.client('sqs')
        queue_url = '***'
        response = sqs.send_message(QueueUrl=queue_url,MessageBody=(str(freq)))
        return response
    return 'Nothing set to run now'

def main():
    schedule = {1:[23],2:[17,23]}
    print(trigger_Lambda(schedule))

main()
