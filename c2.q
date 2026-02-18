/ Command & Control functions 

\d .proc

processlogs:.qi.path(.conf.LOGS;`process)

/ internal functions
{
  os.startproc:$[.qi.WIN;
    {[fileArgs;logfile]
    system "cmd /c if not exist \"",p,"\" mkdir \"",(p:processlogs),"\"";
    system"start /B \"\" cmd /c \"",.conf.QBIN," ",fileArgs," < NUL >> ",logfile," 2>&1\""};

    {[fileArgs;logfile]
      system"mkdir -p ",.qi.spath processlogs;
      system"nohup ",.conf.QBIN," ",fileArgs," < /dev/null >> ",logfile,"  2>&1 &"}];

  os.kill:$[.qi.WIN;
    {[pid]system"taskkill /",.qi.tostr[pid]," /F"};
    {[pid]system"kill ",.qi.tostr pid}];

  os.tail:$[.qi.WIN;
    {[logfile;n]system"cmd /C powershell -Command Get-Content ",.os.towin[logfile]," -Tail ",.qi.tostr n};
    {[logfile;n]system"tail -n ",.qi.tostr[n]," ",logfile}];
  }[]


getpidsx:{[stackname]
  $[count p:.qi.paths[;"*.pid"].qi.local`.qi`pids,stackname;
    ((first` vs last` vs)each p)!(first read0@)each p;
    (0#`)!enlist""]
  }

istack:{x in key 1_key .stacks}
stackprocs:{exec name from .stacks[x]`processes}

/// -- Public Functions

getpids:{getpidsx ACTIVE_STACK}
getpid:{[pname] $[.qi.exists p:.qi.local`.qi`pids,stackname,pname;first read0 p;""]}

up:{[x]
  if[isstack x;:.z.s each stackprocs x];
  / iw - os.startproc 
  }

down:{[x]
  if[isstack x;:.z.s each stackprocs x];
  if[null h:.ipc.conns[x;`handle];:kill x];
  neg[h](`.proc.quit;select name from .proc.self);
  neg[h][];
  }

kill:{
  / iw
  if[isstack x;:os.kill each getpidsx x];
  if[count pid:getpid x;os.kill pid];
  }

bounce:{[x] up x;down x}