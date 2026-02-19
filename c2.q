/ Command & Control functions 

\d .proc

processlogs:.qi.path(.conf.LOGS;`process)
getlog:{[name] .qi.spath(processlogs;` sv name,`log)}

if[0=count .qi.getconf[`QI_CMD;""];
  .conf.QI_CMD:1_{$[.z.o like"m*";"";.qi.WIN;" start /affinity ",string 0b sv -16#(0b vs 0h),(x#1b),y#0b;" taskset -c ","-"sv string(0;x-1)+y]}[.conf.CORES;.conf.FIRST_CORE]," ",.conf.QBIN];


/ internal functions

{
  os.startproc:$[.qi.WIN;
    {[fileArgs;logfile]
    system "cmd /c if not exist \"",p,"\" mkdir \"",(p:processlogs),"\"";
    system"start /B \"\" cmd /c \"",.conf.QBIN," ",fileArgs," < NUL >> ",logfile," 2>&1\""};

    {[fileArgs;logfile]
      system"mkdir -p ",.qi.spath processlogs;
      system"nohup ",.conf.QI_CMD," ",fileArgs," < /dev/null >> ",logfile,"  2>&1 &"}];

  os.kill:$[.qi.WIN;
    {[pid]system"taskkill /",.qi.tostr[pid]," /F"};
    {[pid]system"kill ",.qi.tostr pid}];

  os.tail:$[.qi.WIN;
    {[logfile;n]system"cmd /C powershell -Command Get-Content ",.os.towin[logfile]," -Tail ",.qi.tostr n};
    {[logfile;n]system"tail -n ",.qi.tostr[n]," ",logfile}];
  }[]

isstack:{x in 1_key .stacks}
stackprocs:{exec name from .stacks[x]`processes}

healthpath:{[pname;pid] .qi.local(`.qi;`health;ACTIVE_STACK;pname;pid)}

reporthealth:{
  healthpath[name;`latest]set pid:.z.i;
  healthpath[name;pid]set select time:.z.p,used from .Q.w`;
  }

gethealth:{[pname] 
  d:enlist[`pid]!1#0Ni;
  if[not .qi.exists p:healthpath[pname;`latest];:d];
  if[not .qi.exists hp:healthpath[pname;pid:get p];:d];
  (`pid`path!(pid;hp)),get hp
  }

getpid:{[pname] gethealth[pname]`pid}

/ TODO - handle recycled pids
isup:{[pname] $[null pid:(d:gethealth pname)`pid;0b;os.isup pid;1b;[hdel d`path;0b]]} 

up:{[x]
  if[isstack x;:.z.s each stackprocs x];
  os.startproc["qi.q ",string x;getlog x];
  }

down:{[x]
  if[isstack x;:.z.s each stackprocs x];
  if[null h:.ipc.conns[x;`handle];: os.kill getpids[]x];
  neg[h](`.proc.quit;select name from .proc.self);
  neg[h][];
  }

kill:{
  if[isstack x;:os.kill each getpidsx x];
  if[count pid:getpids[]x;os.kill pid];
  }

os.isup:$[.qi.WIN;
        {[pid] 0<count @[system;"tasklist /FI \"PID eq ",p,"\" | find \"",(p:.qi.tostr pid),"\"";""]};
        {[pid] 0<count @[system;"ps -p ",.qi.tostr pid;""]}];


tailx:{[pname;n]
  if[()~e:entry pname;:notfound pname];
  $[.qi.isfile lf:e`log;system"tail -n ",string[n]," ",lf;"Log file not found ",lf]
 }

tail:{[pname] tailx[pname;.conf.TAIL_ROWS]}

bounce:{[x] up x;down x}
