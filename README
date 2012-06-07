This project is an Erlang binding of the librados on Linux. It tried 
too bind the RADOS C API of the Ceph distributed object store as closely
as possible, while maintaining the Erlang semantics and idioms.

Note that this has been done for my own use, in my own projects. Therefore, 
I have only bound the functions that I needed at the moment. Not all functions
of the librados are done at this point.

This project compiles and runs only on Linux. No other platform is 
supported.

This has been tested on Linux 3.2.x, with Ceph 0.46-1 and Erlang R15B01.

This project is released under the GNU LGPL license. Please refer to

http://www.gnu.org/licenses/lgpl.html

for more information.


--------------------

How to use:

1) Check out the source code with:

     git clone https://github.com/renzhi/erlrados.git

2) Build the NIF functions in C:

     make

   This will create a shared library rados_nif.so

3) Copy the files

     rados.erl
     rados_nif.so

   to your project, and them part of the project.

4) In your Erlang code, before you can connect to your Ceph cluster,
   you need to load the NIF shared library with:

     rados:load("<path/to/rados_nif.so")

   For newer version of Erlang, you can take advantage of the

     -on_load()

   directive. You might want to add the following to rados.erl:

     -on_load(auto_load/0).

     auto_load() ->
         load("./").

   assuming that you are putting rados_nif.so in the same directory
   as your beam files.


Now, to connect to your cluster from Erlang, you can something like this:

   {ok, Cluster} = rados:create(),
   rados:conf_read_file(Cluster, "./etc/ceph.conf"),
   rados:connect(Cluster),

This is assuming that you have your ceph.conf and keyring files under the
etc folder in the same place as where you execute your Erlang code.

To list all pools in your cluster, do:

   {ok, List} = rados:pool_list(Cluster),

To create a new pool, do:

   rados:pool_create(Cluster, Pool),

To create an IO context to the pool, do:

   {ok, IoCtx} = rados:ioctx_create(Cluster, Pool),

Now, you can read from and write to objects in your pool with the
IO context.

Once you are done, you can do:

   rados:ioctx_destroy(IoCtx),

to clean up.

To disconnect from the cluster, do:

   rados:shutdown(Cluster).

More information about the functions are available in rados.erl.

Notes:
  Note that the logger component is lifted from one of my other projects developed
  previously for other purpose. It has half-baked porting to Windows. It probably
  has problems on Windows, but I never had time, or bothered, to do more testing.


xp@renzhi.ca



