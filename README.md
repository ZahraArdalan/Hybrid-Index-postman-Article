# PostMan (Reduced Implementation) — Partition Management & Hybrid Index

A single-machine, dependency-light reference implementation of the core
partition-management and hybrid-indexing techniques described in:

> Jin, J., Fang, Z., Chen, L., Gao, Y. *"PostMan: A Productive System for
> Spatio-temporal Data Management and Analysis."* Data Science and
> Engineering 10, 729–752 (2025). https://doi.org/10.1007/s41019-025-00302-0

This project implements **Section 4** of the paper (Unified Partition
Management and Hybrid Index) end-to-end in pure Python + [Shapely](https://shapely.readthedocs.io/),
without requiring a Spark/Hadoop cluster or GPU hardware, and validates
correctness against brute-force search.

---

## Table of Contents
- [Overview](#overview)
- [What's Implemented](#whats-implemented)
- [What's Out of Scope](#whats-out-of-scope)
- [Project Structure](#project-structure)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Example Output](#example-output)
- [Correctness Tests](#correctness-tests)
- [Mapping to the Paper](#mapping-to-the-paper)
- [License](#license)

---

## Overview

The original PostMan is a distributed spatio-temporal data system built on
Apache Spark + HDFS, with GPU-accelerated operators. Running the full
system requires a multi-node cluster and NVIDIA GPUs, which isn't
practical for a quick evaluation on a single laptop.

This repo instead re-implements the **algorithmic core** of the paper's
partition-management and hybrid-index design (Section 4) as a standalone
Python package:

- Spark RDDs are simulated as in-memory partitioned collections (same API
  shape: `map`, `filter`, `collect`, partitions).
- Spatial partitioning uses an STR (Sort-Tile-Recursive) algorithm — the
  same family of bulk-loading R-tree algorithm the paper uses.
- The hybrid index (global + local) is built with `shapely.strtree.STRtree`
  (itself a bulk-loaded R-tree), avoiding the need for the `rtree` /
  `libspatialindex` C library.
- A small, geographically real demo dataset (NYC landmarks + boroughs)
  stands in for the paper's OSM/Taxi datasets.

## What's Implemented

| Feature | Paper Section | File |
|---|---|---|
| `Vector` element, `STDataset`/`VectorDataset` (RDD simulation, mixin pattern) | Sect. 3.2 | `st_dataset.py` |
| Partition metadata schema (MBR, count, size, vertices, attribute ranges, user-extensible scheme) | Sect. 4.1.i | `partition_manager.py` |
| STR-based spatial repartitioning | Sect. 4.1.ii | `partition_manager.py` |
| Partition persistence (GeoJSON + JSON metadata) & lazy reload | Sect. 4.1.iii | `partition_manager.py` |
| Incremental updates (R*-tree-style insertion + overflow split) | Sect. 4.1.iv | `partition_manager.py` |
| Global index (STR-tree over partition MBRs) | Sect. 4.2.i | `hybrid_index.py` |
| Local index (per-partition STR-tree, built lazily) | Sect. 4.2.ii | `hybrid_index.py` |
| Range query (filter–refine) | Sect. 4.3.1.i | `query_engine.py` |
| kNN query (safe pruning boundary via `maxdist`) | Sect. 4.3.1.ii | `query_engine.py` |
| Vector-to-vector distributed join query | Sect. 4.3.1.iii | `query_engine.py` |
| CSV / GeoJSON loaders | Sect. 7.i | `loaders.py` |

All three query types are validated against brute-force full scans in
`test_correctness.py` — pruning changes performance, **not** results.

## What's Out of Scope

| Item | Status | Reason |
|---|---|---|
| Real Spark/HDFS execution | Simulated in-memory | No cluster available; API shape preserved instead |
| GPU acceleration (Sect. 6) | Not implemented | Requires NVIDIA GPU + RAPIDS/cuDF |
| TPSP phase 2 — executor allocation (Sect. 5) | Not implemented | Only meaningful with multiple real Spark executors; only phase 1 (STR partition generation) is used |
| Raster / `Tile` / `RasterDataset` | Not implemented | Out of the requested scope (Section 4 focuses on vector + hybrid index) |
| Gzip/Bzip2, OBS, Redis-backed global index | Not implemented | Infrastructure details, not core algorithms |
| Real `rtree`/libspatialindex R-tree | Replaced with `shapely.strtree.STRtree` | Not preinstalled / requires C build; STRtree is itself a bulk-loaded R-tree, so it's an algorithmically faithful substitute |

## Project Structure

```
postman_impl/
├── st_dataset.py          # Vector element + STDataset/VectorDataset (RDD simulation)
├── partition_manager.py   # Metadata schema, STR repartitioning, persist/reload, incremental updates
├── hybrid_index.py        # Global index + Local index
├── query_engine.py        # Range / kNN / Join queries (filter-refine)
├── loaders.py             # CSV / GeoJSON loaders
├── generate_data.py       # Generates the demo NYC POI + borough dataset
├── demo.py                # End-to-end walkthrough with stats printed at each stage
├── test_correctness.py    # Validates indexed queries against brute-force

```

## Installation

Requires Python 3.9+.

```bash
git clone <this-repo-url>
cd postman_impl
pip install shapely
```

## Quick Start

```bash
python3 generate_data.py       # 1. generate the demo dataset (once)
python3 demo.py                # 2. run the full walkthrough
python3 test_correctness.py    # 3. verify correctness vs. brute-force
```

## Example Output

```
$ python3 demo.py
...
Sect 4.3.1.i: Range query (filter by global index, refine locally)
QueryStats(total_partitions=16, candidate_partitions=4 [75.0% pruned], records_scanned=750, results=346)

Sect 4.3.1.ii: kNN query (safe pruning boundary via maxdist)
QueryStats(total_partitions=16, candidate_partitions=1 [93.8% pruned], records_scanned=188, results=10)

Sect 4.3.1.iii: Vector-to-vector join query (distributed join)
QueryStats(total_partitions=16, candidate_partitions=16 [0.0% pruned], records_scanned=3080, results=3878)

Sect 4.1.iv: Incremental partition update (R*-tree insertion)
Inserted 50 new records near Times Square.
Partitions before=16, after=20
```

## Correctness Tests

```
$ python3 test_correctness.py
[OK] range_query matches brute force (346 results, 4/16 partitions scanned)
[OK] knn_query matches brute force (k=10, 1/16 partitions scanned)
[OK] join_query matches brute force (3878 pairs)

All correctness tests passed.
```



## License

Educational / academic use. Not affiliated with the original PostMan
authors or Zhejiang University.

---
---

<div dir="rtl" align="right">
<h1 dir="rtl" align="right">پست‌من (نسخه‌ی کوچک‌سازی‌شده) — مدیریت پارتیشن و Hybrid Index</h1>

<p dir="rtl" align="right">
پیاده‌سازی مرجعِ سبک و تک‌ماشینی از هسته‌ی تکنیک‌های مدیریت پارتیشن و
ایندکس ترکیبی توصیف‌شده در مقاله‌ی زیر:
</p>

<blockquote dir="rtl" align="right">
Jin, J., Fang, Z., Chen, L., Gao, Y. <i>"PostMan: A Productive System for
Spatio-temporal Data Management and Analysis."</i> Data Science and
Engineering 10, 729–752 (2025).
<a href="https://doi.org/10.1007/s41019-025-00302-0">https://doi.org/10.1007/s41019-025-00302-0</a>
</blockquote>

<p dir="rtl" align="right">
این پروژه <b>بخش ۴ مقاله</b> (مدیریت یکپارچه‌ی پارتیشن و Hybrid Index) را
به‌طور کامل با پایتون خالص و
<a href="https://shapely.readthedocs.io/">Shapely</a>
پیاده‌سازی کرده، بدون نیاز به کلاستر Spark/Hadoop یا سخت‌افزار GPU، و صحت
آن را در برابر جست‌وجوی brute-force اعتبارسنجی می‌کند.
</p>

<h2 dir="rtl" align="right">فهرست مطالب</h2>
<ul dir="rtl" align="right">
<li><a href="#معرفی">معرفی</a></li>
<li><a href="#چه-چیزی-پیاده‌سازی-شده">چه چیزی پیاده‌سازی شده</a></li>
<li><a href="#چه-چیزی-خارج-از-محدوده-است">چه چیزی خارج از محدوده است</a></li>
<li><a href="#ساختار-پروژه">ساختار پروژه</a></li>
<li><a href="#نصب">نصب</a></li>
<li><a href="#شروع-سریع">شروع سریع</a></li>
<li><a href="#نمونه-خروجی">نمونه خروجی</a></li>
<li><a href="#تست‌های-صحت">تست‌های صحت</a></li>
<li><a href="#تناظر-با-مقاله">تناظر با مقاله</a></li>
</ul>

<h2 dir="rtl" align="right" id="معرفی">معرفی</h2>
<p dir="rtl" align="right">
پست‌من اصلی یک سیستم مدیریت داده‌ی مکانی-زمانی توزیع‌شده روی Apache
Spark + HDFS با عملگرهای شتاب‌گرفته با GPU است. اجرای کامل آن به یک
کلاستر چند-نودی و GPU نیاز دارد که برای ارزیابی سریع روی یک لپ‌تاپ عملی
نیست.
</p>
<p dir="rtl" align="right">
این مخزن به‌جای آن، <b>هسته‌ی الگوریتمی</b> طراحی مدیریت پارتیشن و Hybrid
Index مقاله (بخش ۴) را به‌صورت یک پکیج مستقل پایتون پیاده‌سازی می‌کند:
</p>
<ul dir="rtl" align="right">
<li>RDDهای Spark به‌صورت مجموعه‌های پارتیشن‌بندی‌شده‌ی درون‌حافظه شبیه‌سازی شده‌اند (با همان شکل API: <code>map</code>، <code>filter</code>، <code>collect</code>، پارتیشن‌ها).</li>
<li>پارتیشن‌بندی فضایی از الگوریتم STR (Sort-Tile-Recursive) استفاده می‌کند — همان خانواده‌ی الگوریتم bulk-loading R-tree که مقاله استفاده می‌کند.</li>
<li>Hybrid Index (سراسری + محلی) با <code>shapely.strtree.STRtree</code> ساخته شده (که خودش یک R-tree با بارگذاری دسته‌ای است)، بدون نیاز به کتابخانه‌ی C زبان <code>rtree</code>/<code>libspatialindex</code>.</li>
<li>یک دیتاست دموی کوچک و از نظر جغرافیایی واقعی (لندمارک‌ها و بوروهای نیویورک) جایگزین دیتاست‌های OSM/Taxi مقاله شده است.</li>
</ul>

<h2 dir="rtl" align="right" id="چه-چیزی-پیاده‌سازی-شده">چه چیزی پیاده‌سازی شده</h2>
<table dir="rtl" align="right">
<thead>
<tr><th>قابلیت</th><th>بخش مقاله</th><th>فایل</th></tr>
</thead>
<tbody>
<tr><td>کلاس <code>Vector</code>، <code>STDataset</code>/<code>VectorDataset</code> (شبیه‌سازی RDD، الگوی mixin)</td><td>بخش ۳.۲</td><td><code>st_dataset.py</code></td></tr>
<tr><td>Schema متادیتای پارتیشن (MBR، تعداد، حجم، تعداد رأس، بازه‌ی ویژگی‌ها، قابل‌توسعه توسط کاربر)</td><td>بخش ۴.۱.i</td><td><code>partition_manager.py</code></td></tr>
<tr><td>پارتیشن‌بندی فضایی مبتنی بر STR</td><td>بخش ۴.۱.ii</td><td><code>partition_manager.py</code></td></tr>
<tr><td>ذخیره‌سازی پایدار پارتیشن (GeoJSON + متادیتای JSON) و بازیابی تنبل (lazy)</td><td>بخش ۴.۱.iii</td><td><code>partition_manager.py</code></td></tr>
<tr><td>به‌روزرسانی افزایشی (درج به سبک R*-tree + اسپلیت سرریز)</td><td>بخش ۴.۱.iv</td><td><code>partition_manager.py</code></td></tr>
<tr><td>ایندکس سراسری (STR-tree روی MBR پارتیشن‌ها)</td><td>بخش ۴.۲.i</td><td><code>hybrid_index.py</code></td></tr>
<tr><td>ایندکس محلی (STR-tree per-partition، ساخت تنبل)</td><td>بخش ۴.۲.ii</td><td><code>hybrid_index.py</code></td></tr>
<tr><td>کوئری Range (filter–refine)</td><td>بخش ۴.۳.۱.i</td><td><code>query_engine.py</code></td></tr>
<tr><td>کوئری kNN (مرز هرس ایمن با <code>maxdist</code>)</td><td>بخش ۴.۳.۱.ii</td><td><code>query_engine.py</code></td></tr>
<tr><td>کوئری جوین توزیع‌شده‌ی vector-to-vector</td><td>بخش ۴.۳.۱.iii</td><td><code>query_engine.py</code></td></tr>
<tr><td>لودرهای CSV / GeoJSON</td><td>بخش ۷.i</td><td><code>loaders.py</code></td></tr>
</tbody>
</table>

<p dir="rtl" align="right">
هر سه نوع کوئری در <code>test_correctness.py</code> در برابر جست‌وجوی
brute-force اعتبارسنجی شده‌اند — pruning فقط سرعت را تغییر می‌دهد،
<b>نه</b> نتیجه را.
</p>

<h2 dir="rtl" align="right" id="چه-چیزی-خارج-از-محدوده-است">چه چیزی خارج از محدوده است</h2>
<table dir="rtl" align="right">
<thead>
<tr><th>مورد</th><th>وضعیت</th><th>دلیل</th></tr>
</thead>
<tbody>
<tr><td>اجرای واقعی روی Spark/HDFS</td><td>شبیه‌سازی درون‌حافظه‌ای</td><td>کلاستر در دسترس نبود؛ به‌جایش شکل API حفظ شد</td></tr>
<tr><td>شتاب‌دهی GPU (بخش ۶)</td><td>پیاده نشده</td><td>نیاز به GPU انویدیا + RAPIDS/cuDF</td></tr>
<tr><td>TPSP فاز دوم — تخصیص به executor (بخش ۵)</td><td>پیاده نشده</td><td>فقط با چند executor واقعی معنی دارد؛ فقط فاز اول (تولید پارتیشن STR) استفاده شده</td></tr>
<tr><td>Raster / <code>Tile</code> / <code>RasterDataset</code></td><td>پیاده نشده</td><td>خارج از محدوده‌ی درخواستی (بخش ۴ روی وکتور + Hybrid Index تمرکز دارد)</td></tr>
<tr><td>فشرده‌سازی Gzip/Bzip2، OBS، ایندکس سراسری روی Redis</td><td>پیاده نشده</td><td>جزئیات زیرساختی، نه الگوریتم اصلی</td></tr>
<tr><td>R-tree واقعی با <code>rtree</code>/libspatialindex</td><td>جایگزین با <code>shapely.strtree.STRtree</code></td><td>از پیش نصب نبود / نیاز به کامپایل C دارد؛ STRtree خودش یک R-tree با بارگذاری دسته‌ای است، پس جایگزین معتبری از نظر الگوریتمی است</td></tr>
</tbody>
</table>

<h2 dir="rtl" align="right" id="ساختار-پروژه">ساختار پروژه</h2>

<pre dir="ltr" align="left"><code>postman_impl/
├── st_dataset.py          # عنصر Vector + STDataset/VectorDataset (شبیه‌سازی RDD)
├── partition_manager.py   # Schema متادیتا، پارتیشن‌بندی STR، ذخیره/بازیابی، به‌روزرسانی افزایشی
├── hybrid_index.py        # ایندکس سراسری + ایندکس محلی
├── query_engine.py        # کوئری‌های Range / kNN / Join (filter-refine)
├── loaders.py             # لودرهای CSV / GeoJSON
├── generate_data.py       # ساخت دیتاست دموی POI و بوروهای نیویورک
├── demo.py                # اجرای سرتاسری با چاپ آمار در هر مرحله
├── test_correctness.py    # اعتبارسنجی کوئری‌های ایندکس‌شده در برابر brute-force

</code></pre>

<h2 dir="rtl" align="right" id="نصب">نصب</h2>
<p dir="rtl" align="right">نیازمند پایتون ۳.۹ به بالا.</p>

<pre dir="ltr" align="left"><code>git clone &lt;this-repo-url&gt;
cd postman_impl
pip install shapely
</code></pre>

<h2 dir="rtl" align="right" id="شروع-سریع">شروع سریع</h2>

<pre dir="ltr" align="left"><code>python3 generate_data.py       # ۱. ساخت دیتاست دمو (یک‌بار کافیست)
python3 demo.py                # ۲. اجرای کامل با چاپ آمار
python3 test_correctness.py    # ۳. بررسی صحت نتایج نسبت به brute-force
</code></pre>

<h2 dir="rtl" align="right" id="نمونه-خروجی">نمونه خروجی</h2>

<pre dir="ltr" align="left"><code>$ python3 demo.py
...
Sect 4.3.1.i: Range query (filter by global index, refine locally)
QueryStats(total_partitions=16, candidate_partitions=4 [75.0% pruned], records_scanned=750, results=346)

Sect 4.3.1.ii: kNN query (safe pruning boundary via maxdist)
QueryStats(total_partitions=16, candidate_partitions=1 [93.8% pruned], records_scanned=188, results=10)

Sect 4.3.1.iii: Vector-to-vector join query (distributed join)
QueryStats(total_partitions=16, candidate_partitions=16 [0.0% pruned], records_scanned=3080, results=3878)

Sect 4.1.iv: Incremental partition update (R*-tree insertion)
Inserted 50 new records near Times Square.
Partitions before=16, after=20
</code></pre>

<h2 dir="rtl" align="right" id="تست‌های-صحت">تست‌های صحت</h2>

<pre dir="ltr" align="left"><code>$ python3 test_correctness.py
[OK] range_query matches brute force (346 results, 4/16 partitions scanned)
[OK] knn_query matches brute force (k=10, 1/16 partitions scanned)
[OK] join_query matches brute force (3878 pairs)

All correctness tests passed.
</code></pre>



</div>
