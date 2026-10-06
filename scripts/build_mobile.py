"""Build index.html (mobile app, embedded data) + sw.js + manifest + icons from desktop/questions.js."""
import hashlib, pathlib, struct, zlib

ROOT = pathlib.Path(__file__).resolve().parent.parent
data = (ROOT / "desktop" / "questions.js").read_text(encoding="utf-8")

TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, viewport-fit=cover, maximum-scale=1.0">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<meta name="apple-mobile-web-app-title" content="DE Practice">
<meta name="theme-color" content="#FF3621">
<link rel="manifest" href="manifest.webmanifest">
<link rel="apple-touch-icon" href="icon-180.png">
<title>DE Professional Practice Exams</title>
<style>
  :root{
    --bg:#f5f6f8;--card:#fff;--ink:#1a1a1a;--muted:#6b7280;--line:#e2e4e8;
    --brand:#FF3621;--ok:#1a7f37;--bad:#cf222e;--chip:#eef0f3;--accent:#0b5cad;
    --safe-b:env(safe-area-inset-bottom,0px);--safe-t:env(safe-area-inset-top,0px);
  }
  @media (prefers-color-scheme:dark){:root:not([data-theme=light]){
    --bg:#121418;--card:#1f2228;--ink:#e8eaed;--muted:#9aa0a6;--line:#333842;--chip:#2a2e36;--accent:#6cb2ff;}}
  :root[data-theme=dark]{--bg:#121418;--card:#1f2228;--ink:#e8eaed;--muted:#9aa0a6;--line:#333842;--chip:#2a2e36;--accent:#6cb2ff;}
  *{box-sizing:border-box;-webkit-tap-highlight-color:transparent;}
  html,body{margin:0;height:100%;}
  body{background:var(--bg);color:var(--ink);
    font:16px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;
    overscroll-behavior-y:contain;}
  button,select{font-family:inherit;}
  header{position:sticky;top:0;z-index:20;background:var(--card);border-bottom:1px solid var(--line);
    padding:calc(8px + var(--safe-t)) 12px 8px;}
  .top{display:flex;align-items:center;gap:8px;}
  .seg{display:flex;border:1px solid var(--line);border-radius:9px;overflow:hidden;}
  .seg button{border:0;background:var(--card);color:var(--ink);padding:8px 11px;font-weight:600;font-size:13px;}
  .seg button.on{background:var(--brand);color:#fff;}
  .counter{flex:1;text-align:center;font-weight:700;font-size:15px;}
  .counter small{color:var(--muted);font-weight:400;}
  .iconbtn{border:1px solid var(--line);background:var(--card);color:var(--ink);border-radius:9px;
    width:40px;height:40px;font-size:18px;line-height:1;cursor:pointer;}
  .bar{height:6px;background:var(--chip);border-radius:99px;overflow:hidden;margin-top:8px;}
  .bar>i{display:block;height:100%;width:0;background:var(--accent);transition:width .2s;}
  #panel{display:none;margin-top:10px;border-top:1px solid var(--line);padding-top:10px;}
  #panel.show{display:block;}
  #panel .row{display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin:6px 0;font-size:14px;}
  #panel label{display:inline-flex;align-items:center;gap:6px;}
  #panel button,#panel select{border:1px solid var(--line);background:var(--card);color:var(--ink);
    border-radius:8px;padding:9px 12px;font-size:15px;min-height:40px;}
  #panel .danger{color:var(--bad);border-color:var(--bad);}
  #panel .primary{color:#fff;background:var(--brand);border-color:var(--brand);}
  main{padding:14px 12px calc(96px + var(--safe-b));max-width:760px;margin:0 auto;}
  .badge{display:inline-block;font-size:11px;background:var(--chip);color:var(--muted);padding:3px 9px;border-radius:99px;}
  .qnum{font-weight:800;font-size:15px;margin:10px 0 4px;}
  .qtext{white-space:pre-wrap;margin:6px 0 14px;font-size:17px;line-height:1.55;}
  .opt{display:flex;gap:10px;align-items:flex-start;border:1.5px solid var(--line);border-radius:12px;
    padding:14px;margin:9px 0;cursor:pointer;min-height:52px;background:var(--card);}
  .opt .lk{font-weight:800;flex:0 0 auto;}
  .opt.sel{border-color:var(--accent);background:rgba(11,92,173,.09);}
  .rev .opt.correct{border-color:var(--ok);background:rgba(26,127,55,.15);}
  .rev .opt.wrong{border-color:var(--bad);background:rgba(207,34,46,.15);}
  .rev .opt.correct .lk::after{content:" ✓";color:var(--ok);}
  .rev .opt.wrong .lk::after{content:" ✗";color:var(--bad);}
  .optnote{font-size:13px;color:var(--muted);margin:-4px 0 10px 16px;padding-left:10px;border-left:2px solid var(--line);}
  .optnote.ok{border-color:var(--ok);}.optnote.bad{border-color:var(--bad);}
  .verdict{display:none;font-weight:700;font-size:14px;margin-top:10px;}
  .rev .verdict{display:block;}
  .verdict.ok{color:var(--ok);}.verdict.bad{color:var(--bad);}
  .expl{display:none;margin-top:10px;padding:12px;border-left:3px solid var(--accent);
    background:var(--chip);border-radius:0 10px 10px 0;font-size:15px;}
  .rev .expl{display:block;}
  nav{position:fixed;left:0;right:0;bottom:0;background:var(--card);border-top:1px solid var(--line);
    display:flex;gap:10px;padding:10px 12px calc(10px + var(--safe-b));z-index:20;}
  nav button{flex:1;border:1px solid var(--line);background:var(--card);color:var(--ink);
    border-radius:12px;min-height:50px;font-weight:700;font-size:16px;cursor:pointer;}
  nav button.primary{background:var(--brand);color:#fff;border-color:var(--brand);}
  nav button:disabled{opacity:.4;}
  #results{display:none;}
  .score{font-size:40px;font-weight:900;text-align:center;margin:8px 0;}
  .score.pass{color:var(--ok);}.score.fail{color:var(--bad);}
  .subs{text-align:center;color:var(--muted);font-size:14px;margin-bottom:14px;}
  table{border-collapse:collapse;width:100%;font-size:14px;}
  th,td{text-align:left;padding:8px 8px;border-bottom:1px solid var(--line);}
  td.n{text-align:right;font-variant-numeric:tabular-nums;}
  .rbtn{flex:1;min-height:48px;border:1px solid var(--line);background:var(--card);color:var(--ink);border-radius:12px;font-weight:700;font-size:15px;}
  .empty{text-align:center;color:var(--muted);padding:60px 16px;}
</style>
</head>
<body>
<header>
  <div class="top">
    <div class="seg" id="seg"></div>
    <div class="counter" id="counter">—</div>
    <button class="iconbtn" id="menu" aria-label="Settings">⚙</button>
  </div>
  <div class="bar"><i id="prog"></i></div>
  <div id="panel">
    <div class="row"><label><input type="checkbox" id="study"> <b>Instant feedback</b></label></div>
    <div class="row">Theme:
      <select id="theme"><option value="auto">Auto</option><option value="light">Light</option><option value="dark">Dark</option></select>
      <span id="timer" style="margin-left:auto;color:var(--muted)">120:00</span>
    </div>
    <div class="row">
      <button id="jump">Next unanswered</button>
      <button id="finish" class="primary">Finish</button>
      <button id="reset" class="danger">Reset</button>
    </div>
  </div>
</header>

<main id="main">
  <noscript><div class="empty">This app needs JavaScript. On iPhone, open it in Safari via its URL, not from the Files app preview.</div></noscript>
  <div id="q"></div>
  <div id="results"></div>
</main>

<nav>
  <button id="prev">◀ Previous</button>
  <button id="next" class="primary">Next ▶</button>
</nav>

<script>__DATA__</script>
<script>
(function(){
"use strict";
var DATA=window.SIMULADOS||null;
var LS={g:function(k){try{return JSON.parse(localStorage.getItem(k));}catch(e){return null;}},
        s:function(k,v){try{localStorage.setItem(k,JSON.stringify(v));}catch(e){}},
        d:function(k){try{localStorage.removeItem(k);}catch(e){}}};
var $=function(id){return document.getElementById(id);};
var PASS=0.70;
if(!DATA){$("q").innerHTML='<div class="empty">Data not found.</div>';return;}

var sid=LS.g("depro_current"); if(!DATA[sid]) sid=Object.keys(DATA)[0];
var study=LS.g("depro_study"); if(study===null) study=true;
var idx=0, wrongOnly=false;

var theme=LS.g("depro_theme")||"auto"; applyTheme(theme); $("theme").value=theme;
$("theme").onchange=function(){theme=this.value;LS.s("depro_theme",theme);applyTheme(theme);};
function applyTheme(t){if(t==="auto")document.documentElement.removeAttribute("data-theme");else document.documentElement.setAttribute("data-theme",t);}

$("study").checked=study;
$("study").onchange=function(){study=this.checked;LS.s("depro_study",study);renderQ();};
$("menu").onclick=function(){$("panel").classList.toggle("show");};

function kA(){return "depro_ans_"+sid;} function kS(){return "depro_sub_"+sid;}
function kT(){return "depro_timer_"+sid;} function kI(){return "depro_idx_"+sid;}
function ans(){return LS.g(kA())||{};}
function setAns(a){LS.s(kA(),a);}
function submitted(){return !!LS.g(kS());}
function qs(){return DATA[sid].questions;}
function reveal(q,a){return submitted()||(study&&!!a[q.id]);}

function buildSeg(){
  var s=$("seg");s.innerHTML="";
  Object.keys(DATA).forEach(function(k,i){
    var b=document.createElement("button");
    b.textContent="Exam "+(i+1);
    b.className=k===sid?"on":"";
    b.onclick=function(){sid=k;LS.s("depro_current",sid);idx=LS.g(kI())||0;wrongOnly=false;render();};
    s.appendChild(b);
  });
}

function workList(){
  var a=ans();
  if(wrongOnly&&submitted()) return qs().filter(function(q){return a[q.id]!==q.answer;});
  return qs();
}

function renderQ(){
  $("results").style.display="none";
  $("q").style.display="block";
  var list=workList(); if(list.length===0){list=qs();wrongOnly=false;}
  if(idx>=list.length) idx=list.length-1; if(idx<0) idx=0;
  var q=list[idx], a=ans(), rev=reveal(q,a);
  var box=$("q"); box.className=rev?"rev":"";
  var dom=(q.domain||"")+(q.topic?" — "+q.topic:"");
  var h='<span class="badge">'+esc(dom)+'</span><div class="qnum">Question '+q.id+'</div>';
  h+='<div class="qtext">'+esc(q.text)+'</div>';
  ["A","B","C","D"].forEach(function(L){
    if(q.options[L]==null)return;
    var sel=a[q.id]===L, cls="opt"+(sel?" sel":"");
    if(rev){if(L===q.answer)cls+=" correct";else if(sel)cls+=" wrong";}
    h+='<div class="'+cls+'" data-l="'+L+'"><span class="lk">'+L+')</span><span>'+esc(q.options[L])+'</span></div>';
    if(rev && q.optExpl && q.optExpl[L]) h+='<div class="optnote '+(L===q.answer?"ok":"bad")+'">'+esc(q.optExpl[L])+'</div>';
  });
  var chosen=a[q.id], correct=chosen===q.answer;
  h+='<div class="verdict '+(correct?"ok":"bad")+'">'+(!chosen?"":(correct?"✓ Correct ("+q.answer+")":"✗ You chose "+chosen+" — correct answer is "+q.answer))+'</div>';
  if(q.explanation)h+='<div class="expl"><b>Why:</b> '+esc(q.explanation)+'</div>';
  box.innerHTML=h;
  box.querySelectorAll(".opt").forEach(function(o){
    o.onclick=function(){
      if(submitted())return;
      var aa=ans();aa[q.id]=this.getAttribute("data-l");setAns(aa);
      renderQ();
    };
  });
  updateNav(list); updateBars();
}

function updateNav(list){
  $("prev").disabled=idx<=0;
  $("next").textContent=idx>=list.length-1?"Finish ✓":"Next ▶";
}

function updateBars(){
  var a=ans(),n=qs().filter(function(q){return a[q.id];}).length,t=qs().length;
  $("counter").innerHTML=(idx+1)+"<small>/"+workList().length+"</small>";
  $("prog").style.width=(100*n/t)+"%";
}

$("prev").onclick=function(){if(idx>0){idx--;LS.s(kI(),idx);renderQ();window.scrollTo(0,0);}};
$("next").onclick=function(){
  var list=workList();
  if(idx>=list.length-1){ if(!submitted()) doFinish(); else showResults(); return; }
  idx++;LS.s(kI(),idx);renderQ();window.scrollTo(0,0);
};
$("jump").onclick=function(){
  var a=ans(),list=qs();
  for(var i=0;i<list.length;i++){if(!a[list[i].id]){idx=i;LS.s(kI(),idx);wrongOnly=false;$("panel").classList.remove("show");renderQ();window.scrollTo(0,0);return;}}
  alert("All questions answered ✓");
};
$("finish").onclick=doFinish;
$("reset").onclick=function(){
  if(!confirm("Reset answers, grading and timer for this exam?"))return;
  LS.d(kA());LS.d(kS());LS.d(kT());LS.d(kI());idx=0;wrongOnly=false;$("panel").classList.remove("show");render();
};

function doFinish(){
  var a=ans(),miss=qs().filter(function(q){return !a[q.id];}).length;
  if(!confirm(miss?(miss+" unanswered. Finish anyway?"):"Finish and see results?"))return;
  LS.s(kS(),true);$("panel").classList.remove("show");showResults();
}

function showResults(){
  $("q").style.display="none";
  var r=$("results");r.style.display="block";
  var a=ans(),list=qs();
  var correct=list.filter(function(q){return a[q.id]===q.answer;}).length;
  var pct=correct/list.length,pass=pct>=PASS;
  var dom={};list.forEach(function(q){var d=q.domain||"?";dom[d]=dom[d]||{t:0,c:0};dom[d].t++;if(a[q.id]===q.answer)dom[d].c++;});
  var rows=Object.keys(dom).sort().map(function(d){var o=dom[d];return '<tr><td>'+esc(d)+'</td><td class="n">'+o.c+'/'+o.t+'</td><td class="n">'+Math.round(100*o.c/o.t)+'%</td></tr>';}).join("");
  r.innerHTML='<div class="score '+(pass?"pass":"fail")+'">'+Math.round(pct*100)+'%</div>'
    +'<div class="subs">'+correct+'/'+list.length+' · pass mark ~70% · '+(pass?"above the pass mark 🎯":"below — review the incorrect ones")+'</div>'
    +'<table><thead><tr><th>Domain</th><th class="n">Correct</th><th class="n">%</th></tr></thead><tbody>'+rows+'</tbody></table>'
    +'<div style="display:flex;gap:10px;margin-top:16px">'
    +'<button id="revWrong" class="rbtn">Review incorrect</button>'
    +'<button id="allQ" class="rbtn">Show all</button></div>';
  $("counter").textContent="Results";
  $("revWrong").onclick=function(){
    if(!list.some(function(q){return a[q.id]!==q.answer;})){alert("No incorrect answers 🎯");return;}
    wrongOnly=true;idx=0;renderQ();window.scrollTo(0,0);
  };
  $("allQ").onclick=function(){wrongOnly=false;idx=0;renderQ();window.scrollTo(0,0);};
}

var ti=null;
function timer(){
  clearInterval(ti);
  if(submitted()){$("timer").textContent="—";return;}
  var st=LS.g(kT());if(!st){st=Date.now();LS.s(kT(),st);}
  function tick(){var left=120*60-Math.floor((Date.now()-st)/1000);if(left<=0){left=0;clearInterval(ti);}
    var m=Math.floor(left/60),s=left%60;$("timer").textContent=(m<10?"0":"")+m+":"+(s<10?"0":"")+s;}
  tick();ti=setInterval(tick,1000);
}

function render(){buildSeg();if(submitted())showResults();else renderQ();timer();}
function esc(s){return String(s==null?"":s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");}

idx=LS.g(kI())||0;
render();

if("serviceWorker" in navigator && location.protocol==="https:"){navigator.serviceWorker.register("sw.js");}
})();
</script>
</body>
</html>
"""

MANIFEST = """{
  "name": "DE Professional Practice Exams",
  "short_name": "DE Practice",
  "start_url": "./",
  "scope": "./",
  "display": "standalone",
  "background_color": "#121418",
  "theme_color": "#FF3621",
  "icons": [
    {"src": "icon-180.png", "sizes": "180x180", "type": "image/png"},
    {"src": "icon-512.png", "sizes": "512x512", "type": "image/png"}
  ]
}
"""

# Cache-first so it works offline; the version changes on every build and forces an update.
SW = """const CACHE = "practice-exams-__VER__";
const ASSETS = ["./", "index.html", "manifest.webmanifest", "icon-180.png", "icon-512.png"];
self.addEventListener("install", e => {
  e.waitUntil(caches.open(CACHE).then(c => c.addAll(ASSETS)).then(() => self.skipWaiting()));
});
self.addEventListener("activate", e => {
  e.waitUntil(caches.keys()
    .then(ks => Promise.all(ks.filter(k => k !== CACHE).map(k => caches.delete(k))))
    .then(() => self.clients.claim()));
});
self.addEventListener("fetch", e => {
  if (e.request.method !== "GET") return;
  e.respondWith(caches.match(e.request, {ignoreSearch: true}).then(hit => hit || fetch(e.request)));
});
"""


def png(size, path):
    """Solid lava (#FF3621) icon with a white centre circle; pure PNG, no PIL."""
    r0, g0, b0 = 0xFF, 0x36, 0x21
    c, rad = size / 2, size * 0.28
    rows = []
    for y in range(size):
        row = bytearray([0])
        for x in range(size):
            inside = (x - c + .5) ** 2 + (y - c + .5) ** 2 <= rad * rad
            row += bytes((255, 255, 255) if inside else (r0, g0, b0))
        rows.append(bytes(row))
    raw = zlib.compress(b"".join(rows), 9)

    def chunk(t, d):
        return struct.pack(">I", len(d)) + t + d + struct.pack(">I", zlib.crc32(t + d) & 0xffffffff)

    path.write_bytes(b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack(">IIBBBBB", size, size, 8, 2, 0, 0, 0))
                     + chunk(b"IDAT", raw) + chunk(b"IEND", b""))


html = TEMPLATE.replace("__DATA__", data)
(ROOT / "index.html").write_text(html, encoding="utf-8")
ver = hashlib.sha1(html.encode("utf-8")).hexdigest()[:10]
(ROOT / "sw.js").write_text(SW.replace("__VER__", ver), encoding="utf-8")
(ROOT / "manifest.webmanifest").write_text(MANIFEST, encoding="utf-8")
png(180, ROOT / "icon-180.png")
png(512, ROOT / "icon-512.png")
print(f"index.html {len(html.encode('utf-8'))} bytes | sw cache v{ver} | manifest + icons")
