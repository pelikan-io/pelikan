Histogram
=========

The histogram module provides a histogram that is conceptually similar to [HdrHistogram]_, with modifications to the configurable options and how certain operations are performed.


Background
----------

Recording and reporting quantile metrics are very common and highly valuable in characterizing workload and performance. Usually, system input/output and behavior have a wide but limited range of acceptance, and tracking within this range provides most or all value if the numbers satisfy certain precision requirement. These requirements make libraries such as HdrHistogram highly popular.

Goals
-----

This module aims to provide a simpler implementation, finer-grain configuration, and more efficient runtime compared to the reference implementation provided.


Definition
----------

The histogram is primarily kept in a collection of buckets. The following definitions apply during bucket construction:

- Minimum Resolution (|M|): :math:`M = 2^m`, where |m| is a configurable non-negative integer. the smallest unit of quantification, which is also the smallest bucket width. If the input values are always integers, choosing |m0| would ensure precise recording for the smallest values.
- Minimum Resolution Range (|R|): :math:`R = 2^r-1`, where |r| is a configurable integer with the constraint that :math:`r>m`. This indicates the maximum value Minimum Resolution should extend to.
- Maximum Value (|N|): :math:`N = 2^n-1`, where |n| is a configurable integer with the constraint that :math:`n \ge r`.

There are a few secondary definitions that help us understand the properties of the histogram, and are used often in computation:

- Sustained Precision (|S|): :math:`S = 2^{-(r-m)}`. For example, if |r10| and |m0|, then :math:`S = \frac{1}{1024} \approx 0.1\%`.
- Grouping Factor (|G|): :math:`G = 2^{(r-m-1)} = \frac{2}{S}`.

Design
------

Buckets
^^^^^^^

The buckets are constructed as follows:

+-------------------+------------------------------------------------------+--------------------+--------------------------------------+
| Resolution        | Bucket offset                                        | # Buckets          | Value Range                          |
+-------------------+------------------------------------------------------+--------------------+--------------------------------------+
| :math:`2^{m+0}`   | :math:`[0, 2 \times G)`                              | :math:`2 \times G` | 0 to :math:`2^r - 1`                 |
+-------------------+------------------------------------------------------+--------------------+--------------------------------------+
| :math:`2^{m+1}`   | :math:`[2 \times G, 3 \times G)`                     | :math:`G`          | :math:`2^r` to :math:`2^{r+1} - 1`   |
+-------------------+------------------------------------------------------+--------------------+--------------------------------------+
| ...               | ...                                                  | ...                | ...                                  |
+-------------------+------------------------------------------------------+--------------------+--------------------------------------+
| :math:`2^{m+n-r}` | :math:`[(n - r + 1) \times G, (n - r + 2) \times G)` | :math:`G`          | :math:`2^{n-1}` to :math:`2^{n} - 1` |
+-------------------+------------------------------------------------------+--------------------+--------------------------------------+

Below are a few examples that provide an intuitive understanding of the impact of the primary variables:

#. |m0|, |r10|, |n20|: :math:`M=1, R=1023, N=1048575; G=512`, total number of buckets is 6144. Let's call this our baseline.
#. |m0|, |r10|, :math:`n=30`: :math:`M=1, R=1023, N=1073741823; G=512`, total number of buckets is 11264. Increasing the Maximum Value by a factor of 1024 compared to baseline leads to slightly less than double the buckets.
#. :math:`m=1`, |r10|, |n20|: :math:`M=2, R=1023, N=1048575; G=256`, total number of buckets is 3072. Reducing Minimum Resolution by half also reduces buckets by half.
#. |m0|, :math:`r=9`, |n20|: :math:`M=1, R=511, N=1048575; G=256`, total number of buckets is 3328. Shrinking Minimum Resolution Range by half reduces buckets by less than half.

Counter Type
^^^^^^^^^^^^

Recording counts as integers provides precise reading within counter range, which means it is important to using enough bits to cover the accumulative counts during recording period. This generally means 8-bit, 16-bit, 32-bit, or 64-bit unsigned integers.

Recording counts as floating point numbers increases the dynamic range of individual buckets at the cost of precision. Since HdrHistogram has the concept of precision as a configurable option, it is possible to consider the use of floating point types in conjunction with overall precision.

Below is a table of the range and precision limits for different data types when used to record counts:

+-----------------------+--------------------------+--------------------------+
| Type                  | Range Limit              | Lowest Precision         |
+-----------------------+--------------------------+--------------------------+
| 8-bit integer         | 255                      | \- (precise)             |
+-----------------------+--------------------------+--------------------------+
| 16-bit integer        | 65535                    | \- (precise)             |
+-----------------------+--------------------------+--------------------------+
| 32-bit integer        | :math:`2^{32} - 1`       | \- (precise)             |
+-----------------------+--------------------------+--------------------------+
| 64-bit integer        | :math:`2^{64} - 1`       | \- (precise)             |
+-----------------------+--------------------------+--------------------------+
| 16-bit (half) float   | 65519                    | :math:`\frac{1}{2^{10}}` |
+-----------------------+--------------------------+--------------------------+
| 32-bit (single) float | :math:`\approx 2^{128}`  | :math:`\frac{1}{2^{23}}` |
+-----------------------+--------------------------+--------------------------+
| 64-bit (double) float | :math:`\approx 2^{1024}` | :math:`\frac{1}{2^{52}}` |
+-----------------------+--------------------------+--------------------------+

From the above table, it should be obvious that if considering using 16-bit numbers or less , integer types are strictly superior; for 32- and 64-bit numbers, the choice should be based on potential range of the highest single-bucket count.

Bucket Lookup
^^^^^^^^^^^^^

Bucket lookup is the primary operation for recording a value. The following algorithm only applies to positive integers (i.e. zero values are not allowed). Support for zero value can be added by adding explicit checking of value as a first step into implementation.

We represent the input value as a |n|-digit binary: :math:`V = (a_{n-1}a_{n-2}...a_0)_2`, looking up the right bucket |B| for |V| works as follows:

#. find the highest non-zero digit |h|, :math:`0 \le h \le n-1`;
#. if :math:`h < r`, then :math:`B = V \gg m`;
#. otherwise, :math:`d = h - r + 1, B = (d + 1) \times G + (V - 2^{h}) \gg (m + d)`

*Thoughts on optimization*: On CPUs supporting SSE4, |h| can be most efficiently computed by using ``LZCNT`` instruction.

Below are a few lookup examples assuming |m0|, |r10| (:math:`G=1024`):

#. :math:`V=1`: :math:`h = 0, B = 1 \gg 0 = 1`
#. :math:`V=1023`: :math:`h = 9, B = 1023 \gg 0 = 1023`
#. :math:`V=1024`: :math:`h = 10, d = 1, B = 2 \times 512 + (1024 - 2^{10}) \gg (0 + 1) = 1024`
#. :math:`V=1025`: :math:`h = 10, d = 1, B = 2 \times 512 + (1025 - 2^{10}) \gg (0 + 1) = 1024`
#. :math:`V=1026`: :math:`h = 10, d = 1, B = 2 \times 512 + (1026 - 2^{10}) \gg (0 + 1) = 1025`
#. :math:`V=2048`: :math:`h = 11, d = 2, B = 3 \times 512 + (2048 - 2^{11}) \gg (0 + 2) = 1536`
#. :math:`V=2051`: :math:`h = 11, d = 2, B = 3 \times 512 + (2051 - 2^{11}) \gg (0 + 2) = 1536`
#. :math:`V=2052`: :math:`h = 11, d = 2, B = 3 \times 512 + (2052 - 2^{11}) \gg (0 + 2) = 1537`

Quantile Lookup
^^^^^^^^^^^^^^^

Generally speaking, reporting a particular quantile :math:`q` requires traversing all the buckets once.

There are a couple things to consider during implementation regarding bias in reporting. Because each bucket potentially covers a range, a decision needs to be made about what value in that range to report when we do a quantile lookup. When buckets are not fully populated, we also need to consider how to interpret results that don't fall neatly in a single bucket. For the latter, we use the nearest-rank method to find the nearest bucket with records. Alternatively, it is also possible to extrapolate an in-between value based on existing data, however, this could lead to the confusing situation where the reported value falls into a bucket that no records have fallen, or could possibly fall, into.

*Thoughts on optimization*: To reduce the number of buckets traversed during lookup, one can store the total number of counts, :math:`C`, across all buckets, and return when the buckets traversed so far yields a cumulative count greater than :math:`q \times C` (if traversing from lowest bucket) or :math:`(1 - q) \times C` (if traversing from highest bucket). Further reduction can be achieved by using some type of "sketch" that stores cumulative values across multiple buckets, which allows the cursor to jump over many buckets at a time. The tradeoff is multiple values will need to be updated for each recording, and more space will be used.

There are two typical scenarios where HdrHistogram is deployed. The first one is to check if there is any SLA violation, such as latency at 99.9\%. In this case, the percentile of interest is very close to highest end, so a simple global count and backward traversal can greatly reduce the number of buckets visited. The other one is to create a snapshot of value distribution by reporting several pre-defined percentiles at once, such as `p25`, `p50`, `p75`, `p90`, `p95`, `p99`... In this case, it is probably the most efficient to create APIs that allow multiple quantiles to be reported in a single sweeping trip through all the buckets.


Extension
^^^^^^^^^

- Minimum Bucket (|l|): If a Minimum Bucket |l| is provided that satisfies :math:`l \le (n - r + 2 ) \times G`, all the buckets up to |l| can be skipped. For Bucket Lookup, this means final value of |B| should subtract |l|. For Quantile Lookup, this means scanning |l| fewer buckets.


References
----------
.. [HdrHistogram] `High dynamic range histogram <http://www.hdrhistogram.org/>`_
.. [NearestRank] `Nearest-rank method to calculate percentile <https://en.wikipedia.org/wiki/Percentile#The_nearest-rank_method>`_

.. |B| replace:: :math:`B`
.. |G| replace:: :math:`G`
.. |M| replace:: :math:`M`
.. |N| replace:: :math:`N`
.. |R| replace:: :math:`R`
.. |S| replace:: :math:`S`
.. |V| replace:: :math:`V`
.. |h| replace:: :math:`h`
.. |l| replace:: :math:`l`
.. |m| replace:: :math:`m`
.. |n| replace:: :math:`n`
.. |r| replace:: :math:`r`
.. |m0| replace:: :math:`m=0`
.. |r10| replace:: :math:`r=10`
.. |n20| replace:: :math:`n=20`
