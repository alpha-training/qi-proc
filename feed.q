/ Common library used by websocket feed handlers

.feed.H:0#0i
.feed.pc:{[h] .feed.H:.feed.H except h;.qi.info "tp disconnected - switched to modular mode"}

.feed.upd:{[t;x]
  t insert x;
  if[count .feed.H;
    -25!(.feed.H;(`.u.upd;t;get flip get t));
    delete from t];
  }

.feed.tpreconnect:{
  if[count[.feed.H]<count p:.proc.self.publish_to;
    .feed.H:{x where not null x}.ipc.conn each p];
  }

.feed.start:{[header;url]
    if[.qi.isproc;.feed.tpreconnect[];
      if[not count .feed.H;.qi.info"Could not connect to tp - switched to modular mode"]];
    .qi.info"Connection sequence initiated...";
    if[first c:.qi.try[url;header;0Ni];
      :.qi.info"Connection success"];
    .qi.error err:c 2;
    if[err like"*conn*";
      if[.qi.WIN;
        importx[`fetch;dw:`$"deps-win"];
        .qi.fatal"Try setting the env variable:\n$env:PATH += \";",.qi.ospath[.qi.pkgs dw],"\"; $env:SSL_VERIFY_SERVER = \"NO\""]];
    if[err like"*Protocol*";
      if[not .qi.WIN;
      .qi.fatal"Try setting the env variable:\nexport SSL_VERIFY_SERVER=NO"]];
 }

.event.addhandler[`.z.pc;`.feed.pc]
if[.qi.isproc;.cron.add[`.feed.tpreconnect;.z.p+.conf.FEED_RECONNECT;.conf.FEED_RECONNECT]];