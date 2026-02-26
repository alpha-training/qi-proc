/ Process library
/ Communicate with hub

.qi.import`ipc
.qi.import`cron

\d .proc

stacks,:1#.q
self:``name`stackname`fullname!(::;`;`;`);

quit:{[sendername]
  .qi.info".proc.quit called by ",.qi.tostr[sendername],". Exiting";
  exit 0;
  }

ipc.upd:{
  c:(`fullname xkey .ipc.conns)upsert select name,proc:pkg,stackname,fullname,port from getstacks`;
  .ipc.conns:`name xkey update name:?[stackname=.proc.self.stackname;name;fullname]from c;
  }

init:{[x]
  st:last n:fromfullname x;
  self::``name`stackname`fullname!(::;nm;st;` sv(nm:n 0),st);
  ipc.upd[];
  if[(::)~d:stacks st; '"There are no valid stacks of the name ",string st];
  if[not count me:select from(sp:d`processes)where name=nm;
    show sp;
    '"Could not find a ",string[nm]," process in the ",string[st]," stack"];
  self,:first 0!me;
  if[not count sch:{$[count x;`$lower","vs x;x]}.qi.getopt`schemas;
    sch:(exec pkg from sp)inter exec k from .qi.packages where kind like"feed"];
  .qi.importx[0b]each sch;
  system"p ",.qi.tostr self`port;
  .cron.add[`.proc.reporthealth;0Np;.conf.REPORT_HEALTH_PERIOD];
  .event.addhandler[`.z.exit;`.proc.exit]
  .cron.start`;
 }

load1stack:{[p]
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
  sv[`;`stacks,st:first` vs last` vs p]set cfg,enlist[`processes]!enlist r;
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
  load1stack each get[d][;0];
  if[count err1:sl where max w:(sl:1_key stacks)like/:string[pl:exec k from .qi.packages],'"*";
    '"Cannot have a stack name that is similar to a package name: stacks=",(-3!err1)," packages=",-3!pl where max flip w];
  if[count dupes:select from getstacks[]where 1<(count;i)fby([]stackname;hostname;port);
    show `port xasc dupes;
    '"Duplicate processes found on the same stackname/host/port"];
  ipc.upd[];
  }

getstacks:{raze{[st] `stackname`name`fullname xcols update stackname:st,fullname:` sv'(name,'st)from 0!stacks[st]`processes}each $[null x;1_key stacks;(),x]}

subscribe:{[x]
  sd:x;
  if[any x~/:(`;::);
    if[not nosubs:(::)~sd:self`subscribe_to;
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

processlogs:.qi.path(.conf.LOGS;`process)
getlog:{[x] n:fromfullname x; .qi.path(processlogs;n 1;` sv n[0],`log)}

if[0=count .qi.getconf[`QI_CMD;""];
  .conf.QI_CMD:1_{$[.z.o like"m*";"";.qi.WIN;" start /affinity ",string 0b sv -16#(0b vs 0h),(x#1b),y#0b;" taskset -c ","-"sv string(0;x-1)+y]}[.conf.CORES;.conf.FIRST_CORE]," ",.conf.QBIN];

{
  os.startproc:$[.qi.WIN;
    {[args;logpath] (.qi.info;system)@\:"start /B \"\" cmd /c \"",.conf.QBIN," ",args," < NUL >> ",logpath," 2>&1\"";};
    {[args;logpath] (.qi.info;system)@\:"nohup ",.conf.QI_CMD," ",args," < /dev/null >> ",logpath,"  2>&1 &";}];

  os.kill:$[.qi.WIN;{[pid]system"taskkill /",.qi.tostr[pid]," /F"};{[pid] system"kill -9 ",.qi.tostr pid}];

  os.tail:$[.qi.WIN;
    {[logfile;n]system"cmd /C powershell -Command Get-Content ",.os.towin[logfile]," -Tail ",.qi.tostr n};
    {[logfile;n]system"tail -n ",.qi.tostr[n]," ",logfile}];
  }[]

isstack:{x in 1_key stacks}
fromfullname:{(v 0;.conf.DEFAULT_STACK^last 1_v:` vs x)}  / e.g. `tp1.dev1 -> `tp1`dev1
tofullname:{$[x like"*.*";x;` sv x,.conf.DEFAULT_STACK]}  / e.g. `tp1 -> `tp1.dev1 (or `tp1.dev1 -> `tp1.dev1)
stackprocs:{[st] exec name from .ipc.conns where stackname=st}
healthpath:{[pname;sname;pid] .qi.local(`.qi;`health;sname;pname),pid}

reporthealth:{
  healthpath[nm:self.name;st:self.stackname;`latest]set pd:.z.i;
  healthpath[nm;self.stackname;pd]set d:select time:.z.p,used,heap from .Q.w`;
  if[nm<>`hub;if[isup[`hub;`hub];.ipc.ping[`hub;(`heartbeat;self.fullname;update pid:pd from d)]]];
  }

gethealth:{[pname;sname] 
  d:enlist[`pid]!1#0Ni;
  if[not .qi.exists p:healthpath[pname;sname;`latest];:d];
  if[not .qi.exists hp:healthpath[pname;sname;pid:get p];:d];
  (`pid`path!(pid;hp)),get hp
  }

showstatus:{[x]
  r:select name,stackname,fullname,hostname,port from getstacks`;
  if[not null x;r:$["."in s:.qi.tostr x;select from r where fullname=x;"*"in s;select from r where (stackname like s)|(name like s)|fullname like s;select from r where(stackname=x)|name=x]];
  $[0=count r;.qi.info .qi.tostr[x]," does not match any processes / stacks";
  show update status:`down`up .proc.isup'[name;stackname]from r];
  }

getpid:{[pname;sname] gethealth[pname;sname]`pid}

isup:{[pname;sname] $[null pid:(d:gethealth[pname;sname])`pid;0b;os.isup pid;1b;[hdel d`path;0b]]} 

up:{[x]
  if[isstack x;:.z.s each stackprocs x];
  .qi.os.ensuredir first` vs lp:
  os.startproc[.qi.ospath[.qi.local`qi.q]," ",.qi.tostr x;.qi.spath lp:getlog x];
  }

down:{
    if[isstack x;.z.s each stackprocs x;:(::)];
    nm:$[self.stackname=last n:fromfullname x;n 0;` sv n];
    .ipc.ping[nm;(`.proc.quit;self.name)];
  }

kill:{
  if[(t:type x)within -7 -5h;:os.kill x];
  if[t within 5 7h;:.os.kill each x];
  n:fromfullname x;
  if[x~st:n 1;:.z.s each exec name from .ipc.conns where stackname=st];
  $[null pid:getpid[n 0;st];.qi.error"Could not get pid for ",string x;os.kill pid];
  }

os.isup:$[.qi.WIN;
        {[pid] 0<count @[system;"tasklist /FI \"PID eq ",p,"\" | find \"",(p:.qi.tostr pid),"\"";""]};
        {[pid] 0<count @[system;"ps -p ",.qi.tostr pid;""]}];


tailx:{[pname;n]
  if[()~e:entry pname;:notfound pname];
  $[.qi.exists lf:e`log;system"tail -n ",string[n]," ",lf;"Log file not found ",lf]
 }

tail:{[pname] tailx[pname;.conf.TAIL_ROWS]}

.proc.exit:{if[not null self.name;@[hdel;;`]each .qi.paths[healthpath[self.name;self.stackname;()];(),"*"]]}

loadstacks`