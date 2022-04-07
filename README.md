# Simpe Kafka Test

This is a simple application to check connection establishment and interaction with a running kafka broker.

The application will get `host`, `port`, `topic` and `test-to-run` values via comamnd line arguments or from the properties file within the project under resources directory.

## Configuration modes

There are 2 approach to feed the application with configuration:

### Properties file
There is a `config.properties` under the `resources` directory wich is populated with default values to configure the broker, you can simply change the values based on your kafka setup.

### Command line argument
In this approach, the user should provide configuration inputs via command line when running the application in this order:

`host`, `port`, `topic`, `test-to-run`

### Test-to-run short names
The `test-to-run` configuration input indicates which check to run after running the application, these are the expected values:

- `vc`  : To validate a successful connection establishment.
- `ic`  : To validate a failed connection establishment.
- `ct`  : To validate provided `topic` discovery.
- `ccm` : To validate the connection establishment , topic discovery and consume (already) produced messages.
- `cdm` : To validate the connection establishment , topic discovery and consume (dummy) produced messages.


## Run the application
If there is no running kafka broker on your local, you can issue `docker-compose up` in the root of the project to spin up a local
kafka broker with a default topic name `userTopic`

After making sure there is a running broker, run the `KafKaConsumer`'s main method to listen to the local broker with resolved configuration.

To send some messages manually, you can call the `ProducerUtil`'s `runProducer` with given parameters.
