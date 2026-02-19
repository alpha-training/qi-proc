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
gethealth:{[pname] e:()!();$[null pid:getpid pname;e;.qi.exists f:healthpath[pname;pid];get f;e]}
reporthealth:{healthpath[name;.z.i]set select time:.z.p,used from .Q.w`}
getpid:{[pname] $[.qi.exists p:healthpath[pname;`latest];get p;0Ni]}
savepid:{healthpath[name;`latest]set .z.i}

isup:not null getpid@

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
