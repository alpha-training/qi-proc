/ Process library
/ Communicate with hub

.qi.import`log
.qi.import`ipc
.stacks,:1#.q
.estacks,:1#.q  / example stacks

\d .proc

ACTIVE_STACK:`$getenv`QI_STACK

quit:{[senderinfo]
  .log.info(".proc.quit called - exiting.";senderinfo);
  exit 0;
  }

init:{[]
  if[null st:.conf.DEFAULT_STACK^$[`stackname in k:key o:.qi.opts;`$o`stackname;ACTIVE_STACK];
    '"A stackname argument must be provided"];
  ACTIVE_STACK::st;
  if[e:(::)~d:.stacks st;
    if[(::)~d:.estacks st;
      '"There are no valid stacks of the name ",string st];
    .stacks[st]:d];
  a:d`processes;
  if[not count me:$[cn:count n:o`name;select from a where name=`$n;cp:count pr:o`proc;select from a where pkg=`$pr;()];
    if[me~();assert];
    show a;
    '"Could not find ",$[cn;"name=",n;"pkg=",pr]];
  self::(1#.q),first 0!me;
  `.ipc.conns upsert select name,proc:pkg,port from a where name<>.proc.self`name;
  name::self`name;
  .qi.import self`pkg;
  system"p ",.qi.tostr self`port;
  /name::pname;
 / if[not null hubaddr;
  /  .qi.loadf(.qi.pkgs.proc;`hubclient.q);
  /  hub.init[pname;hubaddr]];
 }
 
loadstack:{[ns;p]
  sp:(a:.qi.readj p)`processes;
  pk:`$get[sp][;`pkg];
  if[count err:pk except `hdb,exec k from .qi.packages;show .qi.packages;'"Invalid package(s): ",","sv string err];
  d:`hostname`base_port!"Sj";
  cfg:{(k#x)$(k:key[x]inter key y)#y}[d;a];
  if[not`hostname in key cfg;cfg:cfg,enlist[`hostname]!enlist`localhost];
  def:`pkg`cmd`hostname`port_offset`taskset`args`depends_on`subscribe_to`port!(`;"";`;0N;"";();();()!();0N);
  pkgs:([]name:key v)!key[def]#/:def,/:get v:sp;
  r:update`$pkg,7h$port_offset,`$depends_on,`$subscribe_to,7h$port from pkgs;
  r:update hostname:cfg`hostname,port:port_offset+cfg`base_port from r where null port,not null port_offset;
  r:update port:cfg`base_port from r where pkg=`hub;
  sv[`;ns,first` vs last` vs p]set d,enlist[`processes]!enlist r;
  }

loadstacks:{[ns;dir]
  if[not count p:.qi.paths[dir;"*.json"];:()];
  d:p group last each ` vs'p;
  if[count dupes:where 1<count each d;
    -1 "\n",.Q.s dupes#d;
    '"Duplicate stack names not allowed"];
  loadstack[ns]each get[d][;0];
  if[count dupes:select from getstacks[ns]where 1<(count;i)fby([]hostname;port);
    show `port xasc dupes;
    '"Duplicate processes found on the same host/port"];
  }

getstacks:{[ns] raze{[d;st] `stackname xcols update stackname:st from 0!d[st]`processes}[d]each 1_key d:get ns}

loadstacks[`.stacks;.conf.STACKS];
loadstacks[`.estacks;` sv .qi.pkgs[`proc],`example_stacks];

subscribe:{[x]
  sd:x;
  if[any x~/:(`;::);
    if[not nosubs:(::)~sd:.proc.self`subscribe_to;
      nosubs:0=count sd];
    if[nosubs;'".proc.subscribe requires a subscribe_to entry in the process config, or a subscription argument"]];
  if[count w:where null h:.ipc.conn each k:key sd;
    "Could not connect to ",","sv string k w];
  {[h;x] 
    t:`;s:`;
    if[not x~a:`$"*";
      if[11=abs tx:type x;t:(),x];
      if[99=tx;
        t:key x;
        s:@[g;where a~'g:get x;:;`]]];
    h({[t;s](.u.sub[t;s];`.u `i`L)};t;s)}'[h;sd]
  }

/
loadstack:{[f]
  procs:(r:.qi.parsej f)`processes;
  if[count invalid:except[p:`$distinct get procs[;`proc]]vp:key .qi.readj[.qi.getindex 0b]`procs;
    :log.error"Invalid process type: ",sv[",";string invalid],". Must be one of: ",","sv string vp];
  .qi.addproc each p;
  .conf,:1#.q;
  d:`version`stack`host`base_port!"*SSj";
  .conf,:cfg:{(k#x)$(k:key[x]inter key y)#y}[d;r];
  def:`proc`cmd`port_offset`taskset`args`depends_on`port!(`;"";0N;"";();();0N);
  procs:([]name:key v)!key[def]#/:def,/:get v:r`processes;
  .conf.procs:update`$proc,7h$port_offset,`$depends_on,7h$port from procs;
  update port:port_offset+cfg`base_port from`.conf.procs where null port,not null port_offset;
  update port:cfg`base_port from`.conf.procs where proc=`c2;
 }