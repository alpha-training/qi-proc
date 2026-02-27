.feed.H:()

.feed.upd:{[t;x]
  t insert x;
  if[count .feed.H;
    neg[.feed.H]@\:(.u.upd;t;get flip t);
    delete from t];
  }

.feed.tpreconnect:{
  if[count[.feed.H]<count p:.proc.self.depends_on;
    .feed.H:{x where not null x}.ipc.conn each p];
  }

.feed.start:{[header;url]
    .feed.tpreconnect[];
    .qi.info "Connection sequence initiated...";
    if[not h:first c:.qi.try[url;header;0Ni];
        .qi.error err:c 2;
        if[err like"*conn*";
            if[.qi.WIN;
              importx[0N;dw:`$"deps-win"];
              .qi.fatal"Try setting the env variable:\n$env:PATH += \";",.qi.ospath[.qi.pkgs dw],"\"; $env:SSL_VERIFY_SERVER = \"NO\""]]
        if[err like"*Protocol*";
            if[.z.o in`l64`m64;
                .qi.fatal"Try setting the env variable:\nexport SSL_VERIFY_SERVER=NO"]]];
    if[h;.qi.info"Connection success"];
 }

if[.qi.isproc;.cron.add[`.feed.tpreconnect;.z.p+.conf.FEED_RECONNECT;.conf.FEED_RECONNECT]];