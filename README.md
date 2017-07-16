NSQCore
================

[![NuGet](https://img.shields.io/nuget/v/)](http://www.nuget.org/packages/)

An [NSQ][nsq] .NET Core 1.0 client library which targets netstandard 1.4
It is only compatible with nsqio version 1.0.0-compat


Usage
-----

Nuget source:

    PM> Install-Package NSQCore

### Producing Messages

    var prod = new NsqProducer("localhost", 4151);
    await prod.PublishAsync("topic1", "hello world" );
	await prod.SetMaxInFlightAsync(1);
    
A connection string looks like this:

    nsqd=localhost:4151;

### Consuming Messages

     var cons = NsqConsumer.Create("lookupd=localhost:4161; topic=topic1; channel=abc");
     await cons.ConnectAndWaitAsync(Handler);
     await cons.SetMaxInFlightAsync(1);
     

	 static async Task Handler(Message message)
     {
         Console.WriteLine("Received: Message={0}", message.Body);
         await message.FinishAsync();
     }
    


Connection string values
------------------------

A connection string looks like this:

    nsqd=localhost:4150;

Or, to use nsqlookupd:

    lookup1:4161; lookup2:4161;

A connection string must specify _either_ an `nsqd` endpoint _or_ `nsqlookupd` endpoints, but not both.

| Setting               | Description                                                                                           |
| --------------------- | ----------------------------------------------------------------------------------------------------- |
| lookupd={endpoints}   | List of `nsqlookupd` servers in the form `hostname:httpport`, e.g., `lookup1:4161;lookup2:4161`       |
| nsqd={endpoints}      | A _single_ `nsqd` servers in the form `hostname:tcpport`, e.g., `nsqd=server1:4150;nsqd=server2:4150` |
| clientId={string}     | A string identifying this client to the `nsqd` server                                                 |
| hostname={string}     | The hostname to identify as (defaults to Environment.MachineName)                                     |
| maxInFlight={int}     | The maximum number of messages this client wants to receive without completion                        |


License
-------

The MIT License. See `LICENSE.md`.


[nsq]: http://nsq.io/
