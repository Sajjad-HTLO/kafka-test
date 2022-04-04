# kafka-test

If there is no running kafka broker on local, issue `docker-compose up` in the root of the project to spin up a local kafka broker and create a topic called `my-topic-1`
Then run the `ConsumerTest`'s main method to listen to the local broker's topic .

To send some messages, run the `ProducerUtil`'s main method to send some messages to the topic.
