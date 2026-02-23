/ Process library
/ Communicate with hub

.qi.import`ipc
.qi.import`cron
.qi.frompkg[`proc;`c2]
.stacks,:1#.q

\d .proc

ACTIVE_STACK:`$getenv`QI_STACK

quit:{[sendername]
  .qi.info".proc.quit called by ",.qi.tostr[sendername],". Exiting";
  exit 0;
  }

init:{[namestack]
  nm:first vp:` vs .qi.tosym namestack;
  if[null st:.conf.DEFAULT_STACK^first[1_vp]^ACTIVE_STACK;
    '"A stackname must be provided"];
  self::``name`stackname`fullname!(::;nm;st;` sv nm,st);
  loadstacks st;
  if[(::)~d:.stacks st; '"There are no valid stacks of the name ",string st];
  if[not count me:select from(sp:d`processes)where name=nm;
    show sp;
    '"Could not find a ",string[nm]," process in the ",string[st]," stack"];
  self,:first 0!me;
  if[`tp=self`pkg;
    if[not count sch:{$[count x;`$lower","vs x;x]}.qi.getopt`schemas;
      if[st=.conf.DEFAULT_STACK;sch:.conf.DEFAULT_SCHEMAS]];
    .qi.importx[0b]each sch];
  ipc.upd select name,proc:pkg,stackname:st,port from sp where name<>nm;
  system"p ",.qi.tostr self`port;
  .cron.start`;
 }

ipc.upd:{[procs]
  c:{xkey[x;y]upsert x xkey z}[`name`stackname;update name:(` vs'name)[;0]from .ipc.conns;procs];
  if[not null st:self.stackname;
    if[.qi.ishub|1<count exec distinct stackname from c where stackname<>`hub;
      c:update name:(` sv'name,'stackname)from c where stackname<>st,name<>`hub]];
  if[not .qi.ishub;if[0=count hb:select from c where name=`hub;c:((0#c)upsert enlist`name`stackname`proc`port!(`hub;`hub;`hub;.conf.HUB_PORT))upsert c]];
  `.ipc.conns upsert 0!c;
  }
 
loadstack:{[p]
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
  sv[`;`.stacks,st:first` vs last` vs p]set cfg,enlist[`processes]!enlist r;
  ipc.upd select name,proc:pkg,stackname:st,port from r;
  }

loadstacks:{[st]
  if[not count p:.qi.paths[.conf.STACKS;"*.json"];
    p,:{.qi.cp[x;(.conf.STACKS;`examples),last ` vs x]}each .qi.paths[.qi.pkgs[`proc],`example_stacks;"*.json"]];
  d:p group last each ` vs'p;
  if[not[st~`]&11=abs type st;d:(` sv'((),st),'`json)#d];
  if[0<count empty:where 0=count each d;'"No stack files found for "," "sv string empty];
  if[count dupes:where 1<count each d;
    -1 "\n",.Q.s dupes#d;
    '"Duplicate stack names not allowed"];
  loadstack each get[d][;0];
  if[count err1:sl where max w:(sl:1_key .stacks)like/:string[pl:exec k from .qi.packages],'"*";
    '"Cannot have a stack name that is similar to a package name: stacks=",(-3!err1)," packages=",-3!pl where max flip w];
  if[count dupes:select from getstacks[]where 1<(count;i)fby([]stackname;hostname;port);
    show `port xasc dupes;
    '"Duplicate processes found on the same stackname/host/port"];
  }

getstacks:{raze{[st] `stackname xcols update stackname:st from 0!.stacks[st]`processes}each 1_key .stacks}

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
    .qi.info"Replaying ",.Q.s1 l;
    -11!l];
  }

.proc.exit:{hdel each .qi.paths[.proc.healthpath[self.name;self.stackname;()];(),"*"]}

.cron.add[`.proc.reporthealth;0Np;.conf.REPORT_HEALTH_PERIOD];
.event.addhandler[`.z.exit;`.proc.exit]