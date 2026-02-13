/********************************************************************
 * sftpPool.js  –  ÚNICO punto de acceso SFTP para todo el proceso
 * ------------------------------------------------------------------
 * • thread-safe (internamente serializa el alta / baja de objetos).
 * • 0 colisiones: cada oper. reserva un “lease” sobre un SftpClient
 *   vivo; si muere durante el uso se crea uno nuevo de forma atómica.
 * • breaker exponencial global + back-off local.
 * • time-outs internos (socket inactivo  > IDLE_MS  ⇒ end()).
 *******************************************************************/

const SftpClient = require('ssh2-sftp-client');
const { EventEmitter } = require('events');
const { promisify }    = require('util');
const path             = require('path').posix;

const cfg = {
  host        : process.env.SFTP_HOST || '10.4.0.2',
  port        : +process.env.SFTP_PORT || 22,
  username    : process.env.SFTP_USER || 'fits',
  password    : process.env.SFTP_PASS || 'CHANGE_ME',
  hostVerifier: () => true,
  readyTimeout: 30_000
};

/* ─────────── constantes pool ─────────── */
const MAX_CONN          = 10;
const CONNECT_GAP_MS    = 1_000;              // separación mínima
const IDLE_MS           = 90_000;             // desconexión tras inactividad
const OP_RETRIES        = 2;
const BREAKER_BASE_MS   = 60_000;
const LOCAL_RETRIES     = 6;
const BACKOFF_MS        = 2_000;

/* ─────────── tiny utils ─────────── */
const sleep = promisify(setTimeout);
const transient = (e) =>
  /ECONN|handshake|keepalive|not connected|socket|channel|no response/i
    .test(String(e||''));                       // cast para undefined

/* ─────────── pool singleton ─────────── */
class SftpPool extends EventEmitter {
  #pool   = [];           // {client, busy, lastUse, ok}
  #next   = 0;
  #lastHS = 0;

  /* breaker global */
  #fails = 0;
  #breakerUntil = 0;

  /* ── log ── */
  #log(...m){ console.error(`[${new Date().toISOString()}][SFTP]`,...m); }

  /* ── entradas seguras ── */
  #newEntry() {
    const entry = { client: new SftpClient(), busy: false, lastUse: 0, ok: false };
    entry.client.on('error',  (e)=>this.#onLow(entry,e));
    entry.client.on('close',  ()=>this.#onLow(entry,'CLOSE'));
    entry.client.on('end',    ()=>this.#onLow(entry,'END'));
    return entry;
  }
  #onLow(entry, evt){
    if(entry.ok){ this.#log(evt); entry.ok=false; }
  }

  /* ── breaker helpers ── */
  async #awaitBreaker(){
    while(this.#breakerUntil > Date.now()) await sleep(this.#breakerUntil-Date.now());
  }
  #failure(){
    this.#fails++; this.#breakerUntil = Date.now()
      + BREAKER_BASE_MS * 2 ** Math.min(this.#fails-1,5);
  }
  #success(){ this.#fails=0; this.#breakerUntil = 0; }

  /* ── handshake ── */
  async #connect(entry){
    await this.#awaitBreaker();

    const gap = CONNECT_GAP_MS - (Date.now() - this.#lastHS);
    if(gap>0) await sleep(gap);
    this.#lastHS = Date.now();

    await entry.client.connect(cfg);
    entry.ok = true;
    this.#success();
  }

  /* ── asegura entry viva ── */
  async #ready(entry){
    if(entry.ok) return;
    for(let i=0;i<LOCAL_RETRIES;i++){
      try{
        if(!entry.ok){               // reconstruye total
          entry.client.removeAllListeners?.();
          entry.client = new SftpClient();
          this.#onLow(entry,'RECREATE');
        }
        await this.#connect(entry);
        return;
      }catch(err){
        if(err.code==='ECONNREFUSED') this.#failure();
        entry.ok=false;
        if(i===LOCAL_RETRIES-1) throw err;
        await sleep(BACKOFF_MS*(i+1));
      }
    }
  }

  /* ── leasing ── */
  async #acquire(){
    // descarta idle muertos
    for(const e of this.#pool){
      if(!e.busy && e.ok && Date.now()-e.lastUse>IDLE_MS){
        e.client.end().catch(()=>{});
        e.ok=false;
      }
    }
    let entry = this.#pool[this.#next++ % this.#pool.length];
    if(!entry || this.#pool.length<MAX_CONN){
      entry = this.#newEntry();
      this.#pool.push(entry);
    }
    await this.#ready(entry);
    entry.busy = true;
    return entry;
  }

  #release(entry){
    entry.busy=false;
    entry.lastUse=Date.now();
  }

  /* ───────── proxy público ───────── */
  #proxy;
  getProxy(){
    if(this.#proxy) return this.#proxy;
    this.#proxy = new Proxy({},{
      get: (_,prop)=>{
        if(['then','catch','finally'].includes(prop)) return undefined;
        return async(...args)=>{
          const entry = await this.#acquire();
          const op = async ()=>{
            for(let i=0;i<=OP_RETRIES;i++){
              try{
                const fn = entry.client[prop];
                const rv = typeof fn==='function' ? await fn.apply(entry.client,args) : fn;
                this.#release(entry); return rv;
              }catch(err){
                if(!transient(err) || i===OP_RETRIES){ this.#release(entry); throw err; }
                entry.ok=false;                       // fuerza reconexión
                if(err.code==='ECONNREFUSED') this.#failure();
                await this.#ready(entry);
              }
            }
          };
          return op();
        };
      }
    });
    return this.#proxy;
  }

  async end(){
    for(const e of this.#pool) try{ e.client.end?.(); }catch{}
    this.#pool.length = 0; this.#proxy=null;
  }
}
const singleton = new SftpPool();

/* ═════════════════ EXPORTS (rutas + singleton) ═════════════════ */
const CAN_BASE_DIR = '/home/fits/lek-files-dev/can';
const CAS_BASE_DIR = '/home/fits/lek-files-dev/cas';

module.exports = {
  /* rutas usadas por el resto de la app */
  CAN_BASE_DIR, CAS_BASE_DIR,
  CAN_BOM_DIR : path.join(CAN_BASE_DIR,'bom'),
  CAS_BOM_DIR : path.join(CAS_BASE_DIR,'bom'),
  CAN_REPO_DIR: path.join(CAN_BASE_DIR,'repo'),
  CAS_REPO_DIR: path.join(CAS_BASE_DIR,'repo'),
  SAP_EXCEL_FOLDER:'sap-files/excel',
  SAP_META_FOLDER :'sap-files/meta',

  /* pool */
  sftpSingleton : { get: ()=>singleton.getProxy(), end: ()=>singleton.end() }
};