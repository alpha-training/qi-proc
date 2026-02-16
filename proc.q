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

init:{[namestack]
  nm:first vp:` vs .qi.tosym namestack;
  if[null st:.conf.DEFAULT_STACK^first[1_vp]^ACTIVE_STACK;
    '"A stackname must be provided"];
  ACTIVE_STACK::st;
  if[e:(::)~d:.stacks st;
    if[(::)~d:.estacks st;
      '"There are no valid stacks of the name ",string st];
    .stacks[st]:d];
  a:d`processes;
  if[not count me:select from a where name=nm;
    show a;
    '"Could not find a ",string[nm]," process in the ",string[st]," stack"];
  self::(1#.q),first 0!me;
  `.ipc.conns upsert select name,proc:pkg,port from a where name<>.proc.self`name;
  name::nm;
  system"p ",.qi.tostr self`port;
 }
 
loadstack:{[ns;p]
  sp:(a:.qi.readj p)`processes;
  pk:`$get[sp][;`pkg];
  if[count err:pk except `hdb,exec k from .qi.packages;show .qi.packages;'"Invalid package(s): ",","sv string err];
  d:`hostname`base_port!("S";7h);
  cfg:{(k#x)$(k:key[x]inter key y)#y}[d;a];
  if[not`hostname in key cfg;cfg:cfg,enlist[`hostname]!enlist`localhost];
  def:`pkg`cmd`hostname`port_offset`taskset`args`depends_on`subscribe_to`port!(`;"";`;0N;"";();();()!();0N);
  pkgs:([]name:key sp)!(key[def]#/:def,/:get sp),'([]options:key[def]_/:get sp);
  r:update`$pkg,7h$port_offset,`$depends_on,`$subscribe_to,7h$port from pkgs;
  r:update hostname:cfg`hostname,port:port_offset+cfg`base_port from r where null port,not null port_offset;
  r:update port:cfg`base_port from r where pkg=`hub;
  sv[`;ns,first` vs last` vs p]set cfg,enlist[`processes]!enlist r;
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

getstacks:{[ns] raze{[d;st] `stackname xcols update stackname:st from 0!d[st]`processes}[d]each 1_key d:$[null ns;`.stacks;get ns]}

loadstacks[`.stacks;.conf.STACKS];
loadstacks[`.estacks;` sv .qi.pkgs[`proc],`example_stacks];

subscribe:{[x]
  sd:x;
  if[any x~/:(`;::);
    if[not nosubs:(::)~sd:.proc.self`subscribe_to;
      nosubs:0=count sd];
    if[nosubs;'".proc.subscribe requires a subscribe_to entry in the process config, or a subscription argument"]];
  if[count w:where null h:.ipc.conn each k:key sd;
    '"Could not connect to ",","sv string k w];
  {[h;x] 
    t:`;s:`;
    if[not x~a:`$"*";
      if[11=abs tx:type x;t:(),x];
      if[99=tx;
        t:key x;
        s:@[g;where a~'g:get x;:;`]]];
    h({[t;s](.u.sub[t;s];`.u `i`L)};t;s)}'[h;sd]
  }

replay:{[x]
  if[99=type x;:.z.s each get x];
  x[0;;0]set'x[0;;1];
  if[not null first l:x 1;
    .log.info"Replaying ",.Q.s1 l;
    -11!l];
  }