

# Module mnesia_rocksdb_lib #
* [Description](#description)
* [Function Index](#index)
* [Function Details](#functions)

RocksDB update wrappers, in separate module for easy tracing and mocking.

<a name="description"></a>

## Description ##
<a name="index"></a>

## Function Index ##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#check_encoding-2">check_encoding/2</a></td><td></td></tr><tr><td valign="top"><a href="#create_mountpoint-1">create_mountpoint/1</a></td><td></td></tr><tr><td valign="top"><a href="#data_mountpoint-1">data_mountpoint/1</a></td><td></td></tr><tr><td valign="top"><a href="#decode-2">decode/2</a></td><td></td></tr><tr><td valign="top"><a href="#decode_key-1">decode_key/1</a></td><td></td></tr><tr><td valign="top"><a href="#decode_key-2">decode_key/2</a></td><td></td></tr><tr><td valign="top"><a href="#decode_val-1">decode_val/1</a></td><td></td></tr><tr><td valign="top"><a href="#decode_val-3">decode_val/3</a></td><td></td></tr><tr><td valign="top"><a href="#default_encoding-3">default_encoding/3</a></td><td></td></tr><tr><td valign="top"><a href="#delete-3">delete/3</a></td><td></td></tr><tr><td valign="top"><a href="#encode-2">encode/2</a></td><td></td></tr><tr><td valign="top"><a href="#encode_key-1">encode_key/1</a></td><td></td></tr><tr><td valign="top"><a href="#encode_key-2">encode_key/2</a></td><td></td></tr><tr><td valign="top"><a href="#encode_val-1">encode_val/1</a></td><td></td></tr><tr><td valign="top"><a href="#encode_val-2">encode_val/2</a></td><td></td></tr><tr><td valign="top"><a href="#keypos-1">keypos/1</a></td><td></td></tr><tr><td valign="top"><a href="#open_rocksdb-3">open_rocksdb/3</a></td><td></td></tr><tr><td valign="top"><a href="#put-4">put/4</a></td><td></td></tr><tr><td valign="top"><a href="#tabname-1">tabname/1</a></td><td></td></tr><tr><td valign="top"><a href="#valid_key_type-2">valid_key_type/2</a></td><td></td></tr><tr><td valign="top"><a href="#valid_obj_type-2">valid_obj_type/2</a></td><td></td></tr><tr><td valign="top"><a href="#write-3">write/3</a></td><td></td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="check_encoding-2"></a>

### check_encoding/2 ###

`check_encoding(Encoding, Attributes) -> any()`

<a name="create_mountpoint-1"></a>

### create_mountpoint/1 ###

`create_mountpoint(Tab) -> any()`

<a name="data_mountpoint-1"></a>

### data_mountpoint/1 ###

`data_mountpoint(Tab) -> any()`

<a name="decode-2"></a>

### decode/2 ###

`decode(Val, X2) -> any()`

<a name="decode_key-1"></a>

### decode_key/1 ###

<pre><code>
decode_key(CodedKey::binary()) -&gt; any()
</code></pre>
<br />

<a name="decode_key-2"></a>

### decode_key/2 ###

`decode_key(CodedKey, Enc) -> any()`

<a name="decode_val-1"></a>

### decode_val/1 ###

<pre><code>
decode_val(CodedVal::binary()) -&gt; any()
</code></pre>
<br />

<a name="decode_val-3"></a>

### decode_val/3 ###

`decode_val(CodedVal, K, Ref) -> any()`

<a name="default_encoding-3"></a>

### default_encoding/3 ###

`default_encoding(X1, Type, As) -> any()`

<a name="delete-3"></a>

### delete/3 ###

`delete(Ref, K, Opts) -> any()`

<a name="encode-2"></a>

### encode/2 ###

`encode(Value, X2) -> any()`

<a name="encode_key-1"></a>

### encode_key/1 ###

<pre><code>
encode_key(Key::any()) -&gt; binary()
</code></pre>
<br />

<a name="encode_key-2"></a>

### encode_key/2 ###

`encode_key(Key, X2) -> any()`

<a name="encode_val-1"></a>

### encode_val/1 ###

<pre><code>
encode_val(Val::any()) -&gt; binary()
</code></pre>
<br />

<a name="encode_val-2"></a>

### encode_val/2 ###

`encode_val(Val, Enc) -> any()`

<a name="keypos-1"></a>

### keypos/1 ###

`keypos(Tab) -> any()`

<a name="open_rocksdb-3"></a>

### open_rocksdb/3 ###

`open_rocksdb(MPd, RdbOpts, CFs) -> any()`

<a name="put-4"></a>

### put/4 ###

`put(Ref, K, V, Opts) -> any()`

<a name="tabname-1"></a>

### tabname/1 ###

`tabname(Tab) -> any()`

<a name="valid_key_type-2"></a>

### valid_key_type/2 ###

`valid_key_type(X1, Key) -> any()`

<a name="valid_obj_type-2"></a>

### valid_obj_type/2 ###

`valid_obj_type(X1, Obj) -> any()`

<a name="write-3"></a>

### write/3 ###

`write(X1, L, Opts) -> any()`

