Segcache is a cache storage engine that delivers high memory efficiency, high
throughput, and excellent scalability for web cache workloads.

The design is optimized for workloads that access predominantly small objects
and use TTL (time-to-live). These workloads, which represent most of what social
media websites, as well as a good portion of web workloads in general, have
historically paid a significant memory overhead due to their small object sizes
and transient nature. In Twitter's case, which was where this design was
originally developed in collaboration with Carnegie Mellon University, the cache
memory footprint was reduced by as much as 60%. This was achieved while
maintaining comparable or better throughput to best-in-kind alternatives, such
as the slab memory allocator in [Memcached](https://memcached.org) and its
cousin in [Twemcache](https://github.com/twitter/twemcache). Segcache also
offers much better (write) scalability compared to Memcached.

The design was first published as a conference paper at NSDI’21, titled “Pelikan
Segcache: a memory-efficient and scalable in-memory key-value cache for small
objects”. It received NSDI Community Award, and the code used in the paper is
merged into Pelikan codebase as of April 2021. A more detailed description can
be found in the form of a [blog post](https://pelikan.io/2021/segcache.html).
