# Gofka [![Build Status](https://secure.travis-ci.org/tsenart/gofka.png)](http://travis-ci.org/tsenart/gofka)

Gofka is a dependency free drop-in replacement for
[Kafka](http://kafka.apache.org/) written in Go. Although still on its early
development stages, I intend it to be:
* Self-contained
* Fast and efficient
* Distributed, fault-tolerant and scalable
* Data compatible with the Kafka data formats
* Wire compatible with the Kafka protocol

> Kafka is a high-throughput publish-subscribe messaging system rethough as a
> distributed commit log. 

Kafka is an incredible piece of software enginneering with many many man months
of deep and subtle thinking put into it. I owe a lot to the [team behind it](https://kafka.apache.org/committers.html)!

This raises the question: **Why Gofka?**
## Rational
### Kafka depends on [Zookeeper](https://zookeeper.apache.org/)
When you're a JVM shop, with a JVM stack, this might come as natural.
However, despite having experience with some of this, I'm moving more
and more into the Go stack. Distributed systems development in Go has
seen the recent adoption of [The Raft Consensus
Algorithm](https://raftconsensus.github.io/) which with the right
implementation, solves all problems Kafka had to solve using Zookeeper.
This means Gofka can be **100%** dependency free.

### Kafka depends on the Java Virtual Machine (JVM)
Without deep expertise, the JVM is a hassle to operate and deploy.
It is very important for Kakfka to control its memory footprint and layout which
is also quite a challange using the JVM. With Go the only artifact to be
shipped and managed is an executable binary (besides configuration,
monitoring, etc... which is orthogonal to this point).
Also, despite being a garbage collected language, the programmer has control over the
memory layout.

### Kafka is good stuff!
After [Jay Kreps'](https://twitter.com/jaykreps) article entitled
[The Log: What every software engineer should know about real-time data's unifying abstraction](http://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying), I fell in love with the
ideas behind Kafka and decided to deeply understand and improve them.
That's what led to this project!

## Contributing
The project is very very early stage at this point. Contributions are
only welcome if their design and rationale is thoroughly discussed first.

## License
```
The MIT License (MIT)

Copyright (c) 2014 Tomás Senart

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
```

