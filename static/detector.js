// Detection Engine v5.0 — JavaScript Port
// Runs entirely in the browser. No server upload needed.

const COMMISSION_HOLD_DAYS = 30;
const PRODUCT_KEYWORDS = ["liposomal magnesium","magnesium + ashwagandha","magnesium+ashwagandha","magasha"];
const WEIGHTS = {
  high_buyer_return_rate:25,repeat_returner:20,high_creator_return_rate:20,
  return_after_commission:30,return_near_commission_window:15,suspicious_reason:10,
  missing_items_cluster_zip:15,bnpl_payment:5,duplicate_return_attempt:25,
  repeat_address_returns:15,state_return_anomaly:10,zip_return_hotspot:15,
  creator_state_concentration:15,creator_self_buy:40,commission_already_paid:25,
  same_day_return:10,sock_puppet_phone:30,sock_puppet_address:20,
};

function clean(val){if(val==null)return"";return String(val).trim().replace(/^\t+|\t+$/g,"").replace(/^\uFEFF/,"");}
function parseDollar(val){val=clean(val);if(!val)return 0;try{return parseFloat(val.replace(/\$|,/g,""))||0;}catch{return 0;}}

function parseDateDDMM(val){
  val=clean(val);if(!val)return null;
  let m=val.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})\s+(\d{1,2}):(\d{2}):(\d{2})\s*(AM|PM)?$/i);
  if(m){let[,d,mo,y,h,min,s,ap]=m;h=parseInt(h);if(ap&&ap.toUpperCase()==='PM'&&h<12)h+=12;if(ap&&ap.toUpperCase()==='AM'&&h===12)h=0;return new Date(+y,+mo-1,+d,h,+min,+s);}
  m=val.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if(m)return new Date(+m[3],+m[2]-1,+m[1]);
  return null;
}
function parseDateMMDD(val){
  val=clean(val);if(!val)return null;
  let m=val.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})\s+(\d{1,2}):(\d{2}):(\d{2})\s*(AM|PM)?$/i);
  if(m){let[,mo,d,y,h,min,s,ap]=m;h=parseInt(h);if(ap&&ap.toUpperCase()==='PM'&&h<12)h+=12;if(ap&&ap.toUpperCase()==='AM'&&h===12)h=0;return new Date(+y,+mo-1,+d,h,+min,+s);}
  m=val.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if(m)return new Date(+m[3],+m[1]-1,+m[2]);
  return null;
}
function isTargetProduct(name){name=(name||"").toLowerCase();return PRODUCT_KEYWORDS.some(kw=>name.includes(kw));}
function riskLabel(s){return s>=60?"CRITICAL":s>=40?"HIGH":s>=20?"MEDIUM":"LOW";}
function recAction(score,reason,status,days){
  if(status==="Completed")return score>=60?"INVESTIGATE":"CLOSED";
  if((status||"").toLowerCase().includes("rejected"))return score>=40?"KEEP REJECTED":"REVIEW REJECTION";
  if(score>=60)return"REJECT";if(score>=40)return"ESCALATE";if(score>=20)return"REVIEW";return"APPROVE";
}
function normPhone(p){if(!p)return"";return p.replace(/\D/g,"").slice(-10);}
function normName(n){if(!n)return"";return n.toLowerCase().replace(/[^a-z0-9 ]/g,"").trim();}
function daysDiff(a,b){if(!a||!b)return null;return Math.floor((b-a)/86400000);}

function parseCSV(text){
  text=text.replace(/^\uFEFF/,"");
  const lines=text.split(/\r?\n/);
  if(lines.length<2)return[];
  function parseLine(line){
    const r=[];let cur="",inQ=false;
    for(let i=0;i<line.length;i++){
      const c=line[i];
      if(c==='"'){if(inQ&&line[i+1]==='"'){cur+='"';i++;}else inQ=!inQ;}
      else if(c===','&&!inQ){r.push(cur.trim());cur="";}
      else cur+=c;
    }
    r.push(cur.trim());return r;
  }
  const headers=parseLine(lines[0]).map(h=>clean(h));
  const rows=[];
  for(let i=1;i<lines.length;i++){
    if(!lines[i].trim())continue;
    const vals=parseLine(lines[i]);
    const row={};
    headers.forEach((h,idx)=>{row[h]=clean(vals[idx]||"");});
    rows.push(row);
  }
  return rows;
}

function runDetection(returnText,affiliateText,ordersText){
  const returns=parseCSV(returnText);
  const affiliates=parseCSV(affiliateText);
  const orders=parseCSV(ordersText);
  const magRA=returns.filter(r=>isTargetProduct(r["Product Name"]));
  const magAff=affiliates.filter(r=>isTargetProduct(r["Product Name"]));
  const magOrd=orders.filter(r=>isTargetProduct(r["Product Name"]));
  const L=buildLookups(returns,affiliates,orders,magRA,magAff,magOrd);
  const magR=magRA.filter(r=>L.cbo[clean(r["Order ID"])]);
  const results=scoreReturns(magR,L);
  const dailyStats=computeDaily(magAff,magR,L);
  const overview=parseOverview(returns,affiliates,orders,magRA,magAff,magOrd);
  const lv={CRITICAL:[],HIGH:[],MEDIUM:[],LOW:[]};
  results.forEach(r=>lv[r.risk_level].push(r));
  const ch=[...lv.CRITICAL,...lv.HIGH];
  const stats={
    total_returns_all:returns.length,total_affiliates_all:affiliates.length,total_orders_all:orders.length,
    mag_returns_all:magRA.length,mag_returns:magR.length,mag_affiliates:magAff.length,mag_orders:magOrd.length,
    critical_count:lv.CRITICAL.length,high_count:lv.HIGH.length,medium_count:lv.MEDIUM.length,low_count:lv.LOW.length,
    total_refund:results.reduce((s,r)=>s+r.refund_amount,0),
    total_commission_risk:results.reduce((s,r)=>s+r.commission_at_risk,0),
    at_risk_refund:ch.reduce((s,r)=>s+r.refund_amount,0),
    at_risk_commission:ch.reduce((s,r)=>s+r.commission_at_risk,0),
    overall_return_rate:L.orr,
    self_buy_count:results.filter(r=>r.flags.includes("SELF-BUY")).length,
    sock_puppet_count:new Set(results.filter(r=>r.flags.includes("SOCK PUPPET")).map(r=>r.buyer_username)).size,
  };
  return{results,stats,dailyStats,overview};
}

function buildLookups(returns,affiliates,orders,magR,magAff,magOrd){
  const cbo={},cOrds={},allC=new Set();
  for(const row of affiliates){
    const oid=clean(row["Order ID"]),cr=clean(row["Creator Username"]);
    if(oid&&cr){
      cbo[oid]={creator:cr,commission_rate:clean(row["Standard commission rate"]),
        est_commission:parseDollar(row["Est. standard commission payment"]),
        actual_commission:parseDollar(row["Actual Commission Payment"]),
        delivery_time:clean(row["Order Delivery Time"]),commission_paid_time:clean(row["Time Commission Paid"])};
      if(isTargetProduct(row["Product Name"])){if(!cOrds[cr])cOrds[cr]=new Set();cOrds[cr].add(oid);}
      allC.add(cr.toLowerCase());
    }
  }
  const abo={};
  for(const row of orders){const oid=clean(row["Order ID"]);if(oid)abo[oid]={buyer_username:clean(row["Buyer Username"]),recipient:clean(row["Recipient"]),phone:clean(row["Phone #"]),state:clean(row["State"]),city:clean(row["City"]),zipcode:clean(row["Zipcode"]),address:clean(row["Address Line 1"]),payment_method:clean(row["Payment Method"]),delivered_time:clean(row["Delivered Time"])};}
  const bOrd={},bRet={};
  for(const row of orders){const b=clean(row["Buyer Username"]),oid=clean(row["Order ID"]);if(b&&oid){if(!bOrd[b])bOrd[b]=new Set();bOrd[b].add(oid);}}
  for(const row of returns){const b=clean(row["Buyer Username"]),oid=clean(row["Order ID"]);if(b&&oid){if(!bRet[b])bRet[b]=new Set();bRet[b].add(oid);}}
  const bRR={};
  for(const row of magR){const b=clean(row["Buyer Username"]),oid=clean(row["Order ID"]);if(b){if(!bRR[b])bRR[b]=[];bRR[b].push(oid);}}
  const cRO={},cOC={};
  for(const[c,s]of Object.entries(cOrds))cOC[c]=s.size;
  for(const row of magR){const oid=clean(row["Order ID"]);if(cbo[oid]){const c=cbo[oid].creator;if(!cRO[c])cRO[c]=new Set();cRO[c].add(oid);}}
  const zipR={},addrR={};
  for(const row of magR){const oid=clean(row["Order ID"]),rsn=clean(row["Return Reason"]),a=abo[oid];if(a){if(a.zipcode){if(!zipR[a.zipcode])zipR[a.zipcode]=[];zipR[a.zipcode].push({order_id:oid,reason:rsn,buyer:clean(row["Buyer Username"])});}if(a.address&&a.address.length>5){const ad=a.address.toUpperCase();if(!addrR[ad])addrR[ad]=new Set();addrR[ad].add(oid);}}}
  const p2b={},r2b={};
  for(const row of magR){const oid=clean(row["Order ID"]),b=clean(row["Buyer Username"]),a=abo[oid];if(a&&b){const ph=normPhone(a.phone),rc=normName(a.recipient);if(ph&&ph.length>=7){if(!p2b[ph])p2b[ph]=new Set();p2b[ph].add(b);}if(rc&&rc.length>3){if(!r2b[rc])r2b[rc]=new Set();r2b[rc].add(b);}}}
  const spPhone={},spName={};
  for(const[ph,bs]of Object.entries(p2b))if(bs.size>=2)for(const b of bs)spPhone[b]={phone:ph,buyers:bs};
  for(const[nm,bs]of Object.entries(r2b))if(bs.size>=2)for(const b of bs)spName[b]={name:nm,buyers:bs};
  const selfB=new Set();
  for(const row of magR){const oid=clean(row["Order ID"]),b=clean(row["Buyer Username"]);if(cbo[oid]){const c=cbo[oid].creator;if(b.toLowerCase()===c.toLowerCase()||allC.has(b.toLowerCase()))selfB.add(oid);}}
  const stO={},stR={};
  for(const row of magOrd){const oid=clean(row["Order ID"]),st=clean(row["State"]);if(st&&oid){if(!stO[st])stO[st]=new Set();stO[st].add(oid);}}
  for(const row of magR){const oid=clean(row["Order ID"]),a=abo[oid];if(a&&a.state){if(!stR[a.state])stR[a.state]=new Set();stR[a.state].add(oid);}}
  const totO=Math.max(1,Object.values(stO).reduce((s,v)=>s+v.size,0));
  const totRt=Math.max(1,Object.values(stR).reduce((s,v)=>s+v.size,0));
  const orr=totRt/totO;
  const srr={};
  for(const st of new Set([...Object.keys(stO),...Object.keys(stR)])){const so=(stO[st]||new Set()).size,sr=(stR[st]||new Set()).size,rate=so>0?sr/so:0;srr[st]={orders:so,returns:sr,rate,is_anomaly:rate>orr*2&&sr>=5};}
  const zO={},zRS={};
  for(const row of magOrd){const zc=clean(row["Zipcode"]),oid=clean(row["Order ID"]);if(zc&&oid){if(!zO[zc])zO[zc]=new Set();zO[zc].add(oid);}}
  for(const row of magR){const oid=clean(row["Order ID"]),a=abo[oid];if(a&&a.zipcode){if(!zRS[a.zipcode])zRS[a.zipcode]=new Set();zRS[a.zipcode].add(oid);}}
  const zipHS=new Set();
  for(const[zc,rs]of Object.entries(zRS)){const zo=(zO[zc]||new Set()).size;if(zo>0&&rs.size/zo>orr*3&&rs.size>=3)zipHS.add(zc);}
  const cRS={};
  for(const row of magR){const oid=clean(row["Order ID"]),a=abo[oid];if(cbo[oid]&&a&&a.state){const c=cbo[oid].creator;if(!cRS[c])cRS[c]={};cRS[c][a.state]=(cRS[c][a.state]||0)+1;}}
  const cConc={};
  for(const[c,sts]of Object.entries(cRS)){const tot=Object.values(sts).reduce((s,v)=>s+v,0);if(tot>=3){const top=Object.entries(sts).sort((a,b)=>b[1]-a[1])[0];if(top[1]/tot>=0.70)cConc[c]={state:top[0],pct:top[1]/tot,count:top[1]};}}
  return{cbo,abo,bOrd,bRet,bRR,cOC,cRO,allC,zipR,addrR,selfB,spPhone,spName,srr,orr,zipHS,zipOrders:Object.fromEntries(Object.entries(zO).map(([k,v])=>[k,v.size])),zipRetCounts:Object.fromEntries(Object.entries(zRS).map(([k,v])=>[k,v.size])),cConc};
}

function scoreReturns(magR,L){
  const SUSP=new Set(["Missing items","Missing package","Missing parts"]);
  const results=[];
  for(const row of magR){
    const oid=clean(row["Order ID"]),buyer=clean(row["Buyer Username"]),reason=clean(row["Return Reason"]),
      retStatus=clean(row["Return Status"]),refund=parseDollar(row["Return unit price"]),
      timeReq=parseDateDDMM(row["Time Requested"]),pay=clean(row["Payment Method"]);
    let score=0;const flags=[];
    const bO=(L.bOrd[buyer]||new Set()).size,bR=(L.bRet[buyer]||new Set()).size,bRate=bO>0?bR/bO:0;
    if(bRate>0.5&&bR>1){score+=WEIGHTS.high_buyer_return_rate;flags.push(`Buyer return rate ${Math.round(bRate*100)}% (${bR}/${bO} orders)`);}
    if(bR>=2){score+=WEIGHTS.repeat_returner;flags.push(`Repeat returner: ${bR} orders returned`);}
    const ci=L.cbo[oid]||{},creator=ci.creator||"";
    let commRisk=0;
    if(creator){const cO=L.cOC[creator]||0,cR=(L.cRO[creator]||new Set()).size;if(cO>0&&cR/cO>0.10&&cR>=2){score+=WEIGHTS.high_creator_return_rate;flags.push(`Creator '${creator}' return rate ${Math.round(cR/cO*100)}% (${cR}/${cO})`);}commRisk=ci.est_commission||ci.actual_commission||0;}
    let delDate=parseDateDDMM(ci.delivery_time||"");const ai=L.abo[oid]||{};
    if(!delDate)delDate=parseDateMMDD(ai.delivered_time||"");
    let daysSince=null;
    if(delDate&&timeReq){daysSince=daysDiff(delDate,timeReq);if(daysSince<0)daysSince=null;else if(daysSince>COMMISSION_HOLD_DAYS){score+=WEIGHTS.return_after_commission;flags.push(`Return ${daysSince}d after delivery (AFTER ${COMMISSION_HOLD_DAYS}d window)`);}else if(daysSince>=25){score+=WEIGHTS.return_near_commission_window;flags.push(`Return ${daysSince}d after delivery (near commission window)`);}}
    const cpDate=parseDateDDMM(ci.commission_paid_time||"");let commPaid=false;
    if(cpDate){commPaid=true;if(timeReq&&timeReq>cpDate){score+=WEIGHTS.commission_already_paid;flags.push(`COMMISSION ALREADY PAID ${daysDiff(cpDate,timeReq)}d before return`);}else if(!timeReq){score+=WEIGHTS.commission_already_paid;flags.push("COMMISSION WAS PAID (timing unclear)");}}
    if(SUSP.has(reason)){score+=WEIGHTS.suspicious_reason;flags.push(`Suspicious reason: '${reason}'`);}
    const zc=ai.zipcode||"";
    if(zc&&L.zipR[zc]){const zm=L.zipR[zc].filter(r=>SUSP.has(r.reason)&&r.order_id!==oid);if(zm.length>=2){score+=WEIGHTS.missing_items_cluster_zip;flags.push(`Zip ${zc}: ${zm.length+1} 'missing items' returns`);}}
    const payFull=pay||ai.payment_method||"";
    if(["klarna","affirm","paylater","pay over time","pay in"].some(kw=>payFull.toLowerCase().includes(kw))){score+=WEIGHTS.bnpl_payment;flags.push(`BNPL payment: ${payFull}`);}
    const brr=L.bRR[buyer]||[];const so=brr.filter(r=>r===oid);if(so.length>1){score+=WEIGHTS.duplicate_return_attempt;flags.push(`Duplicate return: ${so.length} requests on same order`);}
    const addr=(ai.address||"").toUpperCase();if(addr&&addr.length>5&&L.addrR[addr]&&L.addrR[addr].size>=2){score+=WEIGHTS.repeat_address_returns;flags.push(`Address has ${L.addrR[addr].size} return orders`);}
    const state=ai.state||"";if(state&&L.srr[state]&&L.srr[state].is_anomaly){score+=WEIGHTS.state_return_anomaly;const mult=L.orr>0?(L.srr[state].rate/L.orr).toFixed(1):0;flags.push(`State '${state}' return rate ${Math.round(L.srr[state].rate*100)}% (${mult}x avg)`);}
    if(zc&&L.zipHS.has(zc)){const zo=L.zipOrders[zc]||0,zr=L.zipRetCounts[zc]||0;score+=WEIGHTS.zip_return_hotspot;flags.push(`Zip ${zc} hotspot: ${Math.round(zr/zo*100)}% (${zr}/${zo})`);}
    if(creator&&L.cConc[creator]){const cc=L.cConc[creator];score+=WEIGHTS.creator_state_concentration;flags.push(`Creator '${creator}' returns: ${Math.round(cc.pct*100)}% from ${cc.state}`);}
    if(L.selfB.has(oid)){score+=WEIGHTS.creator_self_buy;if(buyer.toLowerCase()===creator.toLowerCase())flags.push(`SELF-BUY: buyer @${buyer} IS the creator`);else flags.push(`SELF-BUY: buyer @${buyer} is a creator buying via @${creator}`);}
    if(daysSince===0){score+=WEIGHTS.same_day_return;flags.push("Same-day return: filed on delivery day");}
    if(L.spPhone[buyer]){const others=[...L.spPhone[buyer].buyers].filter(b=>b!==buyer);if(others.length){score+=WEIGHTS.sock_puppet_phone;flags.push(`SOCK PUPPET: same phone as @${others.slice(0,3).join(', @')}`);}}
    if(L.spName[buyer]){const others=[...L.spName[buyer].buyers].filter(b=>b!==buyer);if(others.length){score+=WEIGHTS.sock_puppet_address;flags.push(`SOCK PUPPET: same recipient '${L.spName[buyer].name}' as @${others.slice(0,3).join(', @')}`);}}
    results.push({risk_level:riskLabel(score),fraud_score:score,recommended_action:recAction(score,reason,retStatus,daysSince),flags:flags.join(" | "),flags_list:flags,return_order_id:clean(row["Return Order ID"]),order_id:oid,buyer_username:buyer,product:(clean(row["Product Name"])||"").slice(0,80),return_reason:reason,return_type:clean(row["Return Type"]),refund_amount:refund,order_amount:parseDollar(row["Order Amount"]),commission_at_risk:commRisk,commission_paid:commPaid?"YES":"NO",return_status:retStatus,return_sub_status:clean(row["Return Sub Status"]),time_requested:timeReq?timeReq.toISOString().slice(0,16).replace('T',' '):"",days_since_delivery:daysSince!==null?daysSince:"",creator,state,city:ai.city||"",zipcode:zc,payment_method:payFull,buyer_note:(clean(row["Buyer Note"])||"").slice(0,200),buyer_unique_orders:bO,buyer_unique_returns:bR,buyer_return_rate:bO>0?`${Math.round(bRate*100)}%`:""});
  }
  results.sort((a,b)=>b.fraud_score-a.fraud_score);
  return results;
}

function computeDaily(magAff,magR,L){
  const d={};
  for(const row of magAff){const dt=parseDateDDMM(row["Time Created"]);if(dt){const k=dt.toISOString().slice(0,10);if(!d[k])d[k]={orders:0,returns:0,refund:0,commission:0};d[k].orders++;}}
  for(const row of magR){const oid=clean(row["Order ID"]);if(!L.cbo[oid])continue;const dt=parseDateDDMM(row["Time Requested"]);if(dt){const k=dt.toISOString().slice(0,10);if(!d[k])d[k]={orders:0,returns:0,refund:0,commission:0};d[k].returns++;d[k].refund+=parseDollar(row["Return unit price"]);const ci=L.cbo[oid];d[k].commission+=ci.est_commission||ci.actual_commission||0;}}
  return Object.entries(d).sort(([a],[b])=>a.localeCompare(b)).map(([date,v])=>({date,...v}));
}

function parseOverview(returns,affiliates,orders,magRA,magAff,magOrd){
  const cbo={};for(const row of magAff){const oid=clean(row["Order ID"]),c=clean(row["Creator Username"]);if(oid&&c)cbo[oid]=c;}
  const magAR=magRA.filter(r=>cbo[clean(r["Order ID"])]);
  const obd={},rbd={},rfbd={};
  for(const row of magAff){const dt=parseDateDDMM(row["Date"]||row["Time Created"]);if(dt){const d=dt.toISOString().slice(0,10);obd[d]=(obd[d]||0)+1;}}
  for(const row of magAR){const dt=parseDateDDMM(row["Time Requested"]);if(dt){const d=dt.toISOString().slice(0,10);rbd[d]=(rbd[d]||0)+1;rfbd[d]=(rfbd[d]||0)+parseDollar(row["Return unit price"]);}}
  const allDates=[...new Set([...Object.keys(obd),...Object.keys(rbd)])].sort();
  const daily=allDates.map(d=>{const o=obd[d]||0,r=rbd[d]||0;return{date:d,orders:o,refunds:r,refund_rate:o>0?`${(r/o*100).toFixed(1)}%`:"0.0%",refund_amount:rfbd[d]||0,top_state:"—",addr_concentration:"—",avg_order_per_addr:"—",max_qty_per_addr:"—"};});
  const totO=daily.reduce((s,d)=>s+d.orders,0),totR=daily.reduce((s,d)=>s+d.refunds,0),dwData=daily.filter(d=>d.orders>0).length;
  const dRates=daily.filter(d=>d.orders>0).map(d=>d.refunds/d.orders*100);
  const avgDR=dRates.length?`${(dRates.reduce((s,v)=>s+v,0)/dRates.length).toFixed(1)}%`:"0.0%";
  const allSt={};for(const row of magOrd){const s=clean(row["State"]);if(s)allSt[s]=(allSt[s]||0)+1;}
  const topSt=Object.entries(allSt).sort((a,b)=>b[1]-a[1])[0];
  const cO={},cR={};
  for(const row of magAff){const c=clean(row["Creator Username"]);if(c)cO[c]=(cO[c]||0)+1;}
  for(const row of magAR){const c=cbo[clean(row["Order ID"])];if(c)cR[c]=(cR[c]||0)+1;}
  const creators=Object.entries(cO).map(([u,o])=>{const r=cR[u]||0;return{username:u,orders:o,returns:r,return_pct:o>0?`${(r/o*100).toFixed(2)}%`:"0.00%"};}).sort((a,b)=>b.returns-a.returns);
  const uO={},uR={};
  for(const row of magOrd){const u=clean(row["Buyer Username"]);if(u)uO[u]=(uO[u]||0)+1;}
  for(const row of magAR){const u=clean(row["Buyer Username"]);if(u)uR[u]=(uR[u]||0)+1;}
  const users=[...new Set([...Object.keys(uO),...Object.keys(uR)])].map(u=>{const o=uO[u]||0,r=uR[u]||0;return{username:u,orders:o,returns:r,return_pct:o>0?`${(r/o*100).toFixed(2)}%`:"N/A"};}).filter(u=>u.orders>0||u.returns>0).sort((a,b)=>b.returns-a.returns);
  function pct(data,p){if(!data.length)return 0;const s=[...data].sort((a,b)=>a-b);const k=(s.length-1)*p/100;const f=Math.floor(k);const c=Math.min(f+1,s.length-1);return s[f]+(k-f)*(s[c]-s[f]);}
  const uRates=users.filter(u=>u.orders>0).map(u=>u.returns/u.orders*100);
  const cRates=creators.filter(c=>c.orders>0).map(c=>c.returns/c.orders*100);
  const uAvg=uRates.length?uRates.reduce((s,v)=>s+v,0)/uRates.length:0;
  const cAvg=cRates.length?cRates.reduce((s,v)=>s+v,0)/cRates.length:0;
  return{daily,summary:{total_orders:String(totO),total_refunds:String(totR),total_refund_rate:totO>0?`${(totR/totO*100).toFixed(2)}%`:"0.0%",avg_daily_return_pct:avgDR,avg_daily_returns:dwData>0?`${(totR/dwData).toFixed(1)}`:"0",top_state:topSt?topSt[0]:"—",top_state_pct:topSt&&totO>0?`${(topSt[1]/totO*100).toFixed(1)}%`:"—",days_of_data:String(daily.length)},creators,creators_with_returns:creators.filter(c=>c.returns>0),users,users_with_returns:users.filter(u=>u.returns>0),user_bench:{avg_refund_pct:`${uAvg.toFixed(2)}%`,p99_refund_pct:`${pct(uRates,99).toFixed(2)}%`,above_avg:String(uRates.filter(r=>r>uAvg).length),above_p99:String(uRates.filter(r=>r>pct(uRates,99)).length)},creator_bench:{avg_refund_pct:`${cAvg.toFixed(2)}%`,p99_refund_pct:`${pct(cRates,99).toFixed(2)}%`,above_avg:String(cRates.filter(r=>r>cAvg).length),above_p95:String(cRates.filter(r=>r>pct(cRates,95)).length)}};
}
