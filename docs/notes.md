# Notes

- __max.poll.interval.ms__ is not exist in our library so it is ok to stay on process.

## Joined new consumer to the group

When a new joiner arrived, it will trigger the partition handler function.  
That means we will skip to commit that messages and new joiner will consume the messages again and take care of commit.  
Messing up the offset of the group is more dangerous problem than duplicate messages.

## Send specific messages to DLQ topic

When using __WrapErrDLQ__ function, it will send the error to dead letter queue topic.  
Usable with index to add specific message to dead letter queue also can give different error message.
