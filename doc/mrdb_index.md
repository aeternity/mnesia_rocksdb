

# Module mrdb_index #
* [Data Types](#types)
* [Function Index](#index)
* [Function Details](#functions)

<a name="types"></a>

## Data Types ##




### <a name="type-index_value">index_value()</a> ###


<pre><code>
index_value() = any()
</code></pre>




### <a name="type-iterator_action">iterator_action()</a> ###


<pre><code>
iterator_action() = <a href="http://www.erlang.org/doc/man/mrdb.html#type-iterator_action">mrdb:iterator_action()</a>
</code></pre>




### <a name="type-ix_iterator">ix_iterator()</a> ###


<pre><code>
ix_iterator() = #mrdb_ix_iter{i = <a href="http://www.erlang.org/doc/man/mrdb.html#type-iterator">mrdb:iterator()</a>, type = set | bag, sub = <a href="http://www.erlang.org/doc/man/mrdb.html#type-ref">mrdb:ref()</a> | pid()}
</code></pre>




### <a name="type-object">object()</a> ###


<pre><code>
object() = tuple()
</code></pre>

<a name="index"></a>

## Function Index ##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#iterator-2">iterator/2</a></td><td></td></tr><tr><td valign="top"><a href="#iterator_close-1">iterator_close/1</a></td><td></td></tr><tr><td valign="top"><a href="#iterator_move-2">iterator_move/2</a></td><td></td></tr><tr><td valign="top"><a href="#with_iterator-3">with_iterator/3</a></td><td></td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="iterator-2"></a>

### iterator/2 ###

<pre><code>
iterator(Tab::<a href="http://www.erlang.org/doc/man/mrdb.html#type-ref_or_tab">mrdb:ref_or_tab()</a>, IxPos::<a href="http://www.erlang.org/doc/man/mrdb.html#type-index_position">mrdb:index_position()</a>) -&gt; {ok, <a href="#type-ix_iterator">ix_iterator()</a>} | {error, term()}
</code></pre>
<br />

<a name="iterator_close-1"></a>

### iterator_close/1 ###

<pre><code>
iterator_close(Mrdb_ix_iter::<a href="#type-ix_iterator">ix_iterator()</a>) -&gt; ok
</code></pre>
<br />

<a name="iterator_move-2"></a>

### iterator_move/2 ###

<pre><code>
iterator_move(Mrdb_ix_iter::<a href="#type-ix_iterator">ix_iterator()</a>, Dir::<a href="#type-iterator_action">iterator_action()</a>) -&gt; {ok, <a href="#type-index_value">index_value()</a>, <a href="#type-object">object()</a>} | {error, term()}
</code></pre>
<br />

<a name="with_iterator-3"></a>

### with_iterator/3 ###

<pre><code>
with_iterator(Tab::<a href="http://www.erlang.org/doc/man/mrdb.html#type-ref_or_tab">mrdb:ref_or_tab()</a>, IxPos::<a href="http://www.erlang.org/doc/man/mrdb.html#type-index_position">mrdb:index_position()</a>, Fun::fun((<a href="#type-ix_iterator">ix_iterator()</a>) -&gt; Res)) -&gt; Res
</code></pre>
<br />

