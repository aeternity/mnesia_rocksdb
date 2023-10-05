

# Module mrdb_stats #
* [Description](#description)
* [Data Types](#types)
* [Function Index](#index)
* [Function Details](#functions)

Statistics API for the mnesia_rocksdb plugin.

<a name="description"></a>

## Description ##
Some counters are maintained for each active alias. Currently, the following
counters are supported:
* inner_retries
* outer_retries

<a name="types"></a>

## Data Types ##




### <a name="type-alias">alias()</a> ###


<pre><code>
alias() = <a href="http://www.erlang.org/doc/man/mnesia_rocksdb.html#type-alias">mnesia_rocksdb:alias()</a>
</code></pre>




### <a name="type-counter">counter()</a> ###


<pre><code>
counter() = atom()
</code></pre>




### <a name="type-counters">counters()</a> ###


<pre><code>
counters() = #{<a href="#type-counter">counter()</a> =&gt; integer()}
</code></pre>




### <a name="type-db_ref">db_ref()</a> ###


<pre><code>
db_ref() = <a href="http://www.erlang.org/doc/man/mrdb.html#type-db_ref">mrdb:db_ref()</a>
</code></pre>




### <a name="type-increment">increment()</a> ###


<pre><code>
increment() = integer()
</code></pre>

<a name="index"></a>

## Function Index ##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#get-1">get/1</a></td><td>Fetches all known counters for <code>Alias</code>, in the form of a map,
<code>#{Counter => Value}</code>.</td></tr><tr><td valign="top"><a href="#get-2">get/2</a></td><td>Fetches the integer value of the known counter <code>Ctr</code> for <code>Alias</code>.</td></tr><tr><td valign="top"><a href="#incr-3">incr/3</a></td><td>Increment <code>Ctr</code> counter for <code>Alias` with increment `N</code>.</td></tr><tr><td valign="top"><a href="#new-0">new/0</a></td><td></td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="get-1"></a>

### get/1 ###

<pre><code>
get(Alias::<a href="#type-alias">alias()</a> | <a href="#type-db_ref">db_ref()</a>) -&gt; <a href="#type-counters">counters()</a>
</code></pre>
<br />

Fetches all known counters for `Alias`, in the form of a map,
`#{Counter => Value}`.

<a name="get-2"></a>

### get/2 ###

<pre><code>
get(Alias::<a href="#type-alias">alias()</a> | <a href="#type-db_ref">db_ref()</a>, Ctr::<a href="#type-counter">counter()</a>) -&gt; integer()
</code></pre>
<br />

Fetches the integer value of the known counter `Ctr` for `Alias`.

<a name="incr-3"></a>

### incr/3 ###

<pre><code>
incr(Alias::<a href="#type-alias">alias()</a> | <a href="#type-db_ref">db_ref()</a>, Ctr::<a href="#type-counter">counter()</a>, N::<a href="#type-increment">increment()</a>) -&gt; ok
</code></pre>
<br />

Increment `Ctr` counter for `Alias` with increment `N`.

Note that the first argument may also be a `db_ref()` map,
corresponding to `mrdb:get_ref({admin, Alias})`.

<a name="new-0"></a>

### new/0 ###

`new() -> any()`

