/*
 * mskql-wasm.js — WASM loader and JS bridge for mskql.
 * Provides env.* imports (math, time, logging) and exposes MskqlDB class.
 */

class MskqlDB {
    constructor() {
        this.instance = null;
        this.memory = null;
        this.encoder = new TextEncoder();
        this.decoder = new TextDecoder();
        this.db = 0; /* pointer to struct database in WASM memory */
    }

    async init(wasmUrl) {
        const imports = {
            env: {
                /* logging */
                js_log: (ptr, len) => {
                    if (this.memory && ptr && len > 0) {
                        const bytes = new Uint8Array(this.memory.buffer, ptr, len);
                        console.log(this.decoder.decode(bytes));
                    }
                },

                /* time */
                js_time: () => Math.floor(Date.now() / 1000),

                js_mktime: (y, m, d, h, min, sec) => {
                    const dt = new Date(y, m - 1, d, h, min, sec);
                    return Math.floor(dt.getTime() / 1000);
                },

                js_localtime: (epoch, outPtr) => {
                    const dt = new Date(epoch * 1000);
                    const view = new Int32Array(this.memory.buffer, outPtr, 7);
                    view[0] = dt.getSeconds();
                    view[1] = dt.getMinutes();
                    view[2] = dt.getHours();
                    view[3] = dt.getDate();
                    view[4] = dt.getMonth();      /* 0-based */
                    view[5] = dt.getFullYear() - 1900;
                    view[6] = dt.getDay();
                },

                /* math */
                js_pow:   (a, b) => Math.pow(a, b),
                js_sqrt:  (x) => Math.sqrt(x),
                js_floor: (x) => Math.floor(x),
                js_ceil:  (x) => Math.ceil(x),
                js_round: (x) => Math.round(x),
                js_fmod:  (x, y) => x % y,
                js_fabs:  (x) => Math.abs(x),
                js_log:   (x) => Math.log(x),
                js_exp:   (x) => Math.exp(x),
                js_sin:   (x) => Math.sin(x),
                js_cos:   (x) => Math.cos(x),

                /* abort */
                js_abort: () => { throw new Error("mskql: abort() called"); },

                /* compiler intrinsic: 128-bit integer multiply (i64 × i64 → i128) */
                __multi3: (result_ptr, a_lo, a_hi, b_lo, b_hi) => {
                    /* Convert to BigInt, multiply, write 128-bit result */
                    const a = (BigInt(a_hi) << 32n) | BigInt(a_lo >>> 0);
                    const b = (BigInt(b_hi) << 32n) | BigInt(b_lo >>> 0);
                    const product = a * b;
                    const mask32 = 0xFFFFFFFFn;
                    const view = new Uint32Array(this.memory.buffer, result_ptr, 4);
                    view[0] = Number(product & mask32);
                    view[1] = Number((product >> 32n) & mask32);
                    view[2] = Number((product >> 64n) & mask32);
                    view[3] = Number((product >> 96n) & mask32);
                },
            }
        };

        const response = await fetch(wasmUrl);
        const bytes = await response.arrayBuffer();
        const result = await WebAssembly.instantiate(bytes, imports);
        this.instance = result.instance;
        this.memory = this.instance.exports.memory;

        /* open database */
        this.db = this.instance.exports.mskql_open();
        if (!this.db) throw new Error("mskql_open failed");
    }

    /* Write a JS string into WASM memory, return [ptr, len] */
    _writeString(str) {
        const encoded = this.encoder.encode(str);
        const ptr = this.instance.exports.mskql_alloc(encoded.length + 1);
        if (!ptr) throw new Error("mskql_alloc failed");
        const view = new Uint8Array(this.memory.buffer, ptr, encoded.length + 1);
        view.set(encoded);
        view[encoded.length] = 0; /* NUL terminator */
        return [ptr, encoded.length];
    }

    /* Read a NUL-terminated C string from WASM memory */
    _readString(ptr, maxLen) {
        const view = new Uint8Array(this.memory.buffer, ptr, maxLen);
        let end = 0;
        while (end < maxLen && view[end] !== 0) end++;
        return this.decoder.decode(view.subarray(0, end));
    }

    /**
     * Execute SQL and return { ok: bool, output: string }
     * Multiple statements separated by ; are executed sequentially.
     */
    exec(sql) {
        const BUF_SIZE = 1024 * 1024; /* 1MB output buffer */
        const outPtr = this.instance.exports.mskql_alloc(BUF_SIZE);
        if (!outPtr) return { ok: false, output: "Failed to allocate output buffer" };

        /* split on semicolons (respecting quotes) */
        const stmts = this._splitStatements(sql);
        let lastOutput = "";
        let lastOk = true;

        for (const stmt of stmts) {
            const trimmed = stmt.trim();
            if (!trimmed) continue;

            const [sqlPtr] = this._writeString(trimmed);
            const rc = this.instance.exports.mskql_exec(this.db, sqlPtr, outPtr, BUF_SIZE);
            this.instance.exports.mskql_dealloc(sqlPtr);

            if (rc < 0) {
                lastOutput = this._readString(outPtr, BUF_SIZE);
                lastOk = false;
            } else {
                lastOutput = this._readString(outPtr, rc > 0 ? rc : BUF_SIZE);
                lastOk = true;
            }
        }

        this.instance.exports.mskql_dealloc(outPtr);
        return { ok: lastOk, output: lastOutput };
    }

    /* Naive statement splitter that respects single-quoted strings */
    _splitStatements(sql) {
        const stmts = [];
        let current = "";
        let inQuote = false;
        for (let i = 0; i < sql.length; i++) {
            const ch = sql[i];
            if (ch === "'" && !inQuote) {
                inQuote = true;
                current += ch;
            } else if (ch === "'" && inQuote) {
                /* check for escaped quote '' */
                if (i + 1 < sql.length && sql[i + 1] === "'") {
                    current += "''";
                    i++;
                } else {
                    inQuote = false;
                    current += ch;
                }
            } else if (ch === ";" && !inQuote) {
                stmts.push(current);
                current = "";
            } else {
                current += ch;
            }
        }
        if (current.trim()) stmts.push(current);
        return stmts;
    }

    /* Reset: close and reopen the database */
    reset() {
        if (this.db) {
            this.instance.exports.mskql_close(this.db);
        }
        this.db = this.instance.exports.mskql_open();
    }

    close() {
        if (this.db) {
            this.instance.exports.mskql_close(this.db);
            this.db = 0;
        }
    }
}
