# gRPC Audio Streaming Server with SQS and SNS Integration

This project is a gRPC server designed to handle audio data streaming, batching it, and then sending it over Amazon Simple Queue Service (SQS). Additionally, it listens for messages on Amazon Simple Notification Service (SNS) and relays them to the connected client if they are addressed to it.

## Features

- **gRPC Audio Streaming**: Utilizes gRPC for efficient communication between clients and the server, enabling real-time audio data streaming.
- **Batching**: Collects incoming audio data streams and batches them together before sending them over SQS, optimizing message delivery.
- **SQS Integration**: Sends batches of audio data over Amazon SQS, ensuring reliable and scalable message queuing.
- **SNS Integration**: Listens for messages on Amazon SNS and relays them to connected clients if they are addressed to them, facilitating seamless communication between clients and external systems.
