#ifndef MSKQL_WASM

#include "pq_reader.h"
#include "column.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>
#include <zstd.h>
#include <zlib.h>

/* ---- Snappy decompressor ---- */

static size_t snappy_varint32(const uint8_t *p, const uint8_t *end, uint32_t *out)
{
    uint32_t v = 0; int shift = 0; const uint8_t *s = p;
    while (p < end) {
        uint8_t b = *p++;
        v |= (uint32_t)(b & 0x7f) << shift;
        if (!(b & 0x80)) { *out = v; return (size_t)(p - s); }
        shift += 7; if (shift >= 32) return 0;
    }
    return 0;
}

static int pq_snappy_decompress(const uint8_t *src, size_t src_len,
                                 uint8_t *dst, size_t dst_cap, size_t *dst_len)
{
    const uint8_t *ip = src, *iend = src + src_len;
    uint32_t ulen; size_t vn = snappy_varint32(ip, iend, &ulen);
    if (!vn || ulen > dst_cap) return -1;
    ip += vn;
    uint8_t *op = dst, *oend = dst + ulen;
    while (ip < iend && op < oend) {
        uint8_t tag = *ip++; uint8_t type = tag & 0x03;
        if (type == 0) { /* literal */
            size_t len = (size_t)(tag >> 2) + 1;
            if (len > 60) {
                size_t ex = len - 60; if (ip + ex > iend) return -1;
                len = 1; for (size_t i = 0; i < ex; i++) len += (size_t)ip[i] << (8*i); ip += ex;
            }
            if (ip + len > iend || op + len > oend) return -1;
            memcpy(op, ip, len); ip += len; op += len;
        } else if (type == 1) {
            size_t len = (size_t)((tag>>2)&7)+4; if (ip >= iend) return -1;
            size_t off = ((size_t)(tag>>5)<<8)|*ip++;
            if (!off || off>(size_t)(op-dst) || op+len>oend) return -1;
            const uint8_t *ref = op-off; while (len-->0) *op++=*ref++;
        } else if (type == 2) {
            size_t len = (size_t)((tag>>2)&0x3f)+1; if (ip+2>iend) return -1;
            size_t off = (size_t)ip[0]|((size_t)ip[1]<<8); ip += 2;
            if (!off || off>(size_t)(op-dst) || op+len>oend) return -1;
            const uint8_t *ref = op-off;
            if (off>=len){memcpy(op,ref,len);op+=len;}else{while(len-->0)*op++=*ref++;}
        } else {
            size_t len = (size_t)((tag>>2)&0x3f)+1; if (ip+4>iend) return -1;
            size_t off = (size_t)ip[0]|((size_t)ip[1]<<8)|((size_t)ip[2]<<16)|((size_t)ip[3]<<24); ip+=4;
            if (!off || off>(size_t)(op-dst) || op+len>oend) return -1;
            const uint8_t *ref = op-off;
            if (off>=len){memcpy(op,ref,len);op+=len;}else{while(len-->0)*op++=*ref++;}
        }
    }
    if ((size_t)(op-dst) != ulen) return -1;
    *dst_len = ulen; return 0;
}

/* ---- Thrift compact decoder ---- */

typedef struct { const uint8_t *buf; size_t pos, len; } TB;
static int tb_byte(TB *t, uint8_t *o) { if (t->pos>=t->len) return -1; *o=t->buf[t->pos++]; return 0; }
static int tb_uvar(TB *t, uint64_t *o) {
    uint64_t v=0; int sh=0;
    while(1){ uint8_t b; if(tb_byte(t,&b)<0)return -1; v|=(uint64_t)(b&0x7f)<<sh; if(!(b&0x80)){*o=v;return 0;} sh+=7; if(sh>=64)return -1; }
}
static int tb_svar(TB *t, int64_t *o) { uint64_t u; if(tb_uvar(t,&u)<0)return -1; *o=(int64_t)((u>>1)^-(int64_t)(u&1)); return 0; }
static int tb_i32(TB *t, int32_t *o) { int64_t v; if(tb_svar(t,&v)<0)return -1; *o=(int32_t)v; return 0; }
static int tb_i64(TB *t, int64_t *o) { return tb_svar(t,o); }
static int tb_bin(TB *t, uint8_t **d, int32_t *l) {
    uint64_t n; if(tb_uvar(t,&n)<0)return -1;
    if(t->pos+n>t->len)return -1; *d=(uint8_t*)(t->buf+t->pos); *l=(int32_t)n; t->pos+=n; return 0;
}
static int tb_fhdr(TB *t, int *ft, int16_t *fid, int16_t prev) {
    uint8_t b; if(tb_byte(t,&b)<0)return -1;
    *ft=b&0x0f;
    if(!*ft){*fid=0;return 0;} /* stop byte */
    int16_t d=(b>>4)&0x0f;
    if(d) *fid=prev+d;
    else { int64_t v; if(tb_svar(t,&v)<0)return -1; *fid=(int16_t)v; }
    return 0;
}
static int tb_skip(TB *t, int ft);
static int tb_skip_struct(TB *t) {
    int16_t prev=0;
    while(1){ int ft; int16_t fid; if(tb_fhdr(t,&ft,&fid,prev)<0)return -1; if(!ft)return 0; prev=fid; if(tb_skip(t,ft)<0)return -1; }
}
static int tb_skip(TB *t, int ft) {
    switch(ft){
    case 1: case 2: return 0; /* bool true/false */
    case 3:{uint8_t b;return tb_byte(t,&b);} /* i8 */
    case 4: case 5: case 6:{int64_t v;return tb_svar(t,&v);} /* i16, i32, i64 */
    case 7: if(t->pos+8>t->len)return -1; t->pos+=8; return 0; /* double */
    case 8:{uint8_t*d;int32_t l;return tb_bin(t,&d,&l);} /* binary */
    case 9: case 10:{if(t->pos>=t->len)return -1; uint8_t h=t->buf[t->pos++]; int et; uint64_t n; /* list, set */
        if((h>>4)==0xf){et=h&0xf;if(tb_uvar(t,&n)<0)return -1;}else{et=h&0xf;n=(h>>4)&0xf;}
        for(uint64_t i=0;i<n;i++) if(tb_skip(t,et)<0)return -1; return 0;}
    case 11:{if(t->pos>=t->len)return -1; uint8_t kv=t->buf[t->pos++]; int kt=kv>>4,vt=kv&0xf; uint64_t n; if(tb_uvar(t,&n)<0)return -1; for(uint64_t i=0;i<n;i++){if(tb_skip(t,kt)<0||tb_skip(t,vt)<0)return -1;} return 0;} /* map */
    case 12: return tb_skip_struct(t); /* struct */
    default: return -1;
    }
}

/* ---- Parquet constants ---- */
#define PQF_PHYS_BOOLEAN  0
#define PQF_PHYS_INT32    1
#define PQF_PHYS_INT64    2
#define PQF_PHYS_INT96    3
#define PQF_PHYS_FLOAT    4
#define PQF_PHYS_DOUBLE   5
#define PQF_PHYS_BYTE_ARRAY 6
#define PQF_PHYS_FIXED_LEN_BYTE_ARRAY 7

#define PQF_COMPRESS_UNCOMPRESSED 0
#define PQF_COMPRESS_SNAPPY 1
#define PQF_COMPRESS_GZIP   2
#define PQF_COMPRESS_ZSTD   6

#define PQF_PAGE_DATA       0
#define PQF_PAGE_DICTIONARY 2
#define PQF_PAGE_DATA_V2    3

#define PQF_ENC_PLAIN       0
#define PQF_ENC_PLAIN_DICT  2
#define PQF_ENC_RLE         3
#define PQF_ENC_BIT_PACKED  4
#define PQF_ENC_RLE_DICT    8

#define PQF_CONV_NA           -1
#define PQF_CONV_UTF8          0
#define PQF_CONV_MAP           1
#define PQF_CONV_MAP_KEY_VALUE 2
#define PQF_CONV_LIST          3
#define PQF_CONV_ENUM          4
#define PQF_CONV_DECIMAL       5
#define PQF_CONV_DATE          6
#define PQF_CONV_TIME_MILLIS   7
#define PQF_CONV_TIME_MICROS   8
#define PQF_CONV_TS_MILLIS     9
#define PQF_CONV_TS_MICROS     10
#define PQF_CONV_INT_8         15
#define PQF_CONV_INT_16        16
#define PQF_CONV_INT_32        17
#define PQF_CONV_INT_64        18
#define PQF_CONV_UINT_8        11
#define PQF_CONV_UINT_16       12
#define PQF_CONV_UINT_32       13
#define PQF_CONV_UINT_64       14
#define PQF_CONV_JSON          19
#define PQF_CONV_BSON          20
#define PQF_CONV_INTERVAL      21

#define PQ_DATE_OFFSET   10957
#define PQ_USEC_OFFSET   (PQ_DATE_OFFSET * 86400LL * 1000000LL)

/* ---- Internal structures ---- */

struct pq_col_meta {
    char   *name;
    int32_t phys_type;
    int32_t converted_type;
    int     is_optional;
    int     log_string, log_date, log_time, log_ts, log_int, log_int64;
};

struct pq_chunk_meta {
    int64_t data_page_offset;
    int64_t dict_page_offset;
    int64_t total_compressed_size;
    int32_t compression;
};

struct pq_row_group_meta {
    int64_t             num_rows;
    struct pq_chunk_meta *chunks;
};

struct pq_reader {
    FILE                    *fp;
    int32_t                  ncols;
    int64_t                  total_rows;
    int32_t                  num_row_groups;
    struct pq_col_meta      *cols;
    struct pq_row_group_meta *rgs;
    enum column_type         *mskql_types;
    enum pq_phys_type        *phys_types;
};

/* ---- Type mapping ---- */

static enum column_type pq_map_type(const struct pq_col_meta *m)
{
    if (m->log_string) return COLUMN_TYPE_TEXT;
    if (m->log_date)   return COLUMN_TYPE_DATE;
    if (m->log_time)   return COLUMN_TYPE_TIME;
    if (m->log_ts)     return COLUMN_TYPE_TIMESTAMP;
    if (m->log_int)    return m->log_int64 ? COLUMN_TYPE_BIGINT : COLUMN_TYPE_INT;
    switch (m->converted_type) {
    case PQF_CONV_UTF8: case PQF_CONV_ENUM: case PQF_CONV_JSON: case PQF_CONV_BSON: return COLUMN_TYPE_TEXT;
    case PQF_CONV_DATE: return COLUMN_TYPE_DATE;
    case PQF_CONV_TIME_MILLIS: case PQF_CONV_TIME_MICROS: return COLUMN_TYPE_TIME;
    case PQF_CONV_TS_MILLIS: case PQF_CONV_TS_MICROS: return COLUMN_TYPE_TIMESTAMP;
    case PQF_CONV_DECIMAL: return COLUMN_TYPE_NUMERIC;
    case PQF_CONV_INT_8: case PQF_CONV_INT_16: case PQF_CONV_INT_32:
    case PQF_CONV_UINT_8: case PQF_CONV_UINT_16: case PQF_CONV_UINT_32: return COLUMN_TYPE_INT;
    case PQF_CONV_INT_64: case PQF_CONV_UINT_64: return COLUMN_TYPE_BIGINT;
    case PQF_CONV_INTERVAL: return COLUMN_TYPE_INTERVAL;
    case PQF_CONV_NA: case PQF_CONV_MAP: case PQF_CONV_MAP_KEY_VALUE: case PQF_CONV_LIST: break;
    }
    switch (m->phys_type) {
    case PQF_PHYS_BOOLEAN: return COLUMN_TYPE_BOOLEAN;
    case PQF_PHYS_INT32:   return COLUMN_TYPE_INT;
    case PQF_PHYS_INT64:   return COLUMN_TYPE_BIGINT;
    case PQF_PHYS_INT96:   return COLUMN_TYPE_TIMESTAMP;
    case PQF_PHYS_FLOAT:   return COLUMN_TYPE_FLOAT;
    case PQF_PHYS_DOUBLE:  return COLUMN_TYPE_FLOAT;
    case PQF_PHYS_BYTE_ARRAY: return COLUMN_TYPE_TEXT;
    case PQF_PHYS_FIXED_LEN_BYTE_ARRAY: return COLUMN_TYPE_TEXT;
    }
    return COLUMN_TYPE_TEXT;
}

static enum pq_phys_type pq_map_phys(int32_t p)
{
    switch (p) {
    case PQF_PHYS_BOOLEAN: return PQ_PHYS_BOOLEAN;
    case PQF_PHYS_INT32:   return PQ_PHYS_INT32;
    case PQF_PHYS_INT64:   return PQ_PHYS_INT64;
    case PQF_PHYS_INT96:   return PQ_PHYS_INT96;
    case PQF_PHYS_FLOAT:   return PQ_PHYS_FLOAT;
    case PQF_PHYS_DOUBLE:  return PQ_PHYS_DOUBLE;
    case PQF_PHYS_BYTE_ARRAY: return PQ_PHYS_BYTE_ARRAY;
    case PQF_PHYS_FIXED_LEN_BYTE_ARRAY: return PQ_PHYS_FIXED_LEN_BYTE_ARRAY;
    }
    return PQ_PHYS_BYTE_ARRAY;
}

/* ---- Footer parsing ---- */

static int parse_schema_elem(TB *t, struct pq_col_meta *out, int *is_leaf)
{
    memset(out, 0, sizeof(*out)); out->converted_type = PQF_CONV_NA; out->phys_type = -1; *is_leaf = 0;
    int16_t prev = 0;
    while (t->pos < t->len) {
        int ft; int16_t fid; if (tb_fhdr(t,&ft,&fid,prev)<0) return -1; if (!ft) break; prev=fid;
        switch (fid) {
        case 1: { int32_t v; if (tb_i32(t,&v)<0) return -1; out->phys_type=v; *is_leaf=1; break; }
        case 2: { int32_t v; if (tb_i32(t,&v)<0) return -1; (void)v; break; } /* type_length */
        case 3: { int32_t v; if (tb_i32(t,&v)<0) return -1; out->is_optional=(v==1); break; }
        case 4: { uint8_t *d; int32_t l; if (tb_bin(t,&d,&l)<0) return -1;
                  out->name=(char*)malloc((size_t)l+1); if(!out->name)return -1;
                  memcpy(out->name,d,(size_t)l); out->name[l]='\0'; break; }
        case 5: { int32_t v; if (tb_i32(t,&v)<0) return -1; (void)v; break; } /* num_children */
        case 6: { int32_t v; if (tb_i32(t,&v)<0) return -1; out->converted_type=v; break; }
        case 10: { /* LogicalType union struct */
            int16_t lp=0;
            while (t->pos<t->len) {
                int lft; int16_t lfid; if(tb_fhdr(t,&lft,&lfid,lp)<0)return -1; if(!lft)break; lp=lfid;
                switch(lfid){
                case 1: out->log_string=1; if(tb_skip_struct(t)<0)return -1; break;
                case 6: out->log_date=1;   if(tb_skip_struct(t)<0)return -1; break;
                case 7: out->log_time=1;   if(tb_skip_struct(t)<0)return -1; break;
                case 8: out->log_ts=1;     if(tb_skip_struct(t)<0)return -1; break;
                case 10: { /* INTEGER: is_signed + bitWidth */
                    out->log_int=1; int16_t ip=0;
                    while(t->pos<t->len){int ift;int16_t ifid;if(tb_fhdr(t,&ift,&ifid,ip)<0)return -1;if(!ift)break;ip=ifid;
                        if(ifid==2){int32_t bw;if(tb_i32(t,&bw)<0)return -1;out->log_int64=(bw==64);}
                        else if(tb_skip(t,ift)<0)return -1;} break;}
                default: if(tb_skip(t,lft)<0)return -1; break;
                }
            }
            break;
        }
        default: if (tb_skip(t,ft)<0) return -1; break;
        }
    }
    return 0;
}

static int parse_col_meta(TB *t, struct pq_chunk_meta *out)
{
    memset(out,0,sizeof(*out));
    int16_t prev=0;
    while(t->pos<t->len){
        int ft;int16_t fid;if(tb_fhdr(t,&ft,&fid,prev)<0)return -1;if(!ft)break;prev=fid;
        switch(fid){
        case 4:{int32_t v;if(tb_i32(t,&v)<0)return -1;out->compression=v;break;}
        case 6:{int64_t v;if(tb_i64(t,&v)<0)return -1;(void)v;break;} /* total_uncompressed_size */
        case 7:{int64_t v;if(tb_i64(t,&v)<0)return -1;out->total_compressed_size=v;break;}
        case 9:{int64_t v;if(tb_i64(t,&v)<0)return -1;out->data_page_offset=v;break;}
        case 11:{int64_t v;if(tb_i64(t,&v)<0)return -1;out->dict_page_offset=v;break;}
        default:if(tb_skip(t,ft)<0)return -1;break;
        }
    }
    return 0;
}

static int parse_col_chunk(TB *t, struct pq_chunk_meta *out)
{
    int16_t prev=0;
    while(t->pos<t->len){
        int ft;int16_t fid;if(tb_fhdr(t,&ft,&fid,prev)<0)return -1;if(!ft)break;prev=fid;
        switch(fid){
        case 3:if(parse_col_meta(t,out)<0)return -1;break;
        default:if(tb_skip(t,ft)<0)return -1;break;
        }
    }
    return 0;
}

static int read_list_header(TB *t, int *etype, uint64_t *count)
{
    if(t->pos>=t->len)return -1;
    uint8_t h=t->buf[t->pos++];
    if((h>>4)==0xf){*etype=h&0xf;return tb_uvar(t,count);}
    *etype=h&0xf; *count=(h>>4)&0xf; return 0;
}

static int parse_row_group(TB *t, struct pq_row_group_meta *rg, int32_t ncols)
{
    rg->chunks=(struct pq_chunk_meta*)calloc((size_t)ncols,sizeof(struct pq_chunk_meta));
    if(!rg->chunks)return -1;
    int32_t col_idx=0;
    int16_t prev=0;
    while(t->pos<t->len){
        int ft;int16_t fid;if(tb_fhdr(t,&ft,&fid,prev)<0)return -1;if(!ft)break;prev=fid;
        switch(fid){
        case 1:{ int et; uint64_t cnt; if(read_list_header(t,&et,&cnt)<0)return -1; (void)et;
            for(uint64_t i=0;i<cnt;i++){
                if(col_idx<ncols){if(parse_col_chunk(t,&rg->chunks[col_idx++])<0)return -1;}
                else if(tb_skip_struct(t)<0)return -1;
            } break;}
        case 3:{int64_t v;if(tb_i64(t,&v)<0)return -1;rg->num_rows=v;break;}
        default:if(tb_skip(t,ft)<0)return -1;break;
        }
    }
    return 0;
}


static int read_footer(pq_reader_t *r)
{
    if(fseek(r->fp,0,SEEK_END)<0)return -1;
    long fsz=ftell(r->fp);
    if(fsz<12)return -1;
    uint8_t tail[8];
    if(fseek(r->fp,fsz-8,SEEK_SET)<0)return -1;
    if((long)fread(tail,1,8,r->fp)!=8)return -1;
    if(memcmp(tail+4,"PAR1",4)!=0)return -1;
    uint32_t flen=(uint32_t)tail[0]|((uint32_t)tail[1]<<8)|((uint32_t)tail[2]<<16)|((uint32_t)tail[3]<<24);
    if((long)flen>fsz-8)return -1;
    uint8_t *fbuf=(uint8_t*)malloc(flen);
    if(!fbuf)return -1;
    if(fseek(r->fp,fsz-8-(long)flen,SEEK_SET)<0){free(fbuf);return -1;}
    if(fread(fbuf,1,flen,r->fp)!=flen){free(fbuf);return -1;}

    TB t={fbuf,0,flen};

    /* Temp schema storage */
    int32_t scap=64;
    struct pq_col_meta *selems=(struct pq_col_meta*)malloc((size_t)scap*sizeof(*selems));
    int *sleaf=(int*)malloc((size_t)scap*sizeof(int));
    int32_t scount=0;
    if(!selems||!sleaf)goto fail;

    int16_t prev=0;
    while(t.pos<t.len){
        int ft;int16_t fid;if(tb_fhdr(&t,&ft,&fid,prev)<0)goto fail;if(!ft)break;prev=fid;
        switch(fid){
        case 1:{int32_t v;if(tb_i32(&t,&v)<0)goto fail;break;} /* version */
        case 2:{ /* schema list */
            int et;uint64_t cnt;if(read_list_header(&t,&et,&cnt)<0)goto fail;(void)et;
            for(uint64_t i=0;i<cnt;i++){
                if(scount>=scap){scap*=2;
                    selems=(struct pq_col_meta*)realloc(selems,(size_t)scap*sizeof(*selems));
                    sleaf=(int*)realloc(sleaf,(size_t)scap*sizeof(int));
                    if(!selems||!sleaf)goto fail;}
                if(parse_schema_elem(&t,&selems[scount],&sleaf[scount])<0)goto fail;
                scount++;
            }
            break;}
        case 3:{int64_t v;if(tb_i64(&t,&v)<0)goto fail;r->total_rows=v;break;}
        case 4:{ /* row_groups list */
            int et;uint64_t cnt;if(read_list_header(&t,&et,&cnt)<0)goto fail;(void)et;
            r->num_row_groups=(int32_t)cnt;
            r->rgs=(struct pq_row_group_meta*)calloc(cnt,sizeof(*r->rgs));
            if(!r->rgs)goto fail;
            /* ncols not known yet — use scount as safe upper bound */
            for(uint64_t i=0;i<cnt;i++) if(parse_row_group(&t,&r->rgs[i],scount)<0)goto fail;
            break;}
        default:if(tb_skip(&t,ft)<0)goto fail;break;
        }
    }

    /* Count leaf columns */
    int32_t ncols=0;
    for(int32_t i=0;i<scount;i++) if(sleaf[i]) ncols++;
    r->ncols=ncols;
    r->cols=(struct pq_col_meta*)malloc((size_t)ncols*sizeof(*r->cols));
    r->mskql_types=(enum column_type*)malloc((size_t)ncols*sizeof(*r->mskql_types));
    r->phys_types=(enum pq_phys_type*)malloc((size_t)ncols*sizeof(*r->phys_types));
    if(!r->cols||!r->mskql_types||!r->phys_types)goto fail;
    {
        int32_t ci=0;
        for(int32_t i=0;i<scount;i++){
            if(!sleaf[i]){free(selems[i].name);continue;}
            r->cols[ci]=selems[i];
            r->mskql_types[ci]=pq_map_type(&selems[i]);
            r->phys_types[ci]=pq_map_phys(selems[i].phys_type);
            ci++;
        }
    }
    free(selems); free(sleaf); free(fbuf); return 0;
fail:
    if(selems){for(int32_t i=0;i<scount;i++)free(selems[i].name);free(selems);}
    free(sleaf); free(fbuf); return -1;
}

/* ---- Public open/close/accessors ---- */

pq_reader_t *pq_open(const char *path)
{
    pq_reader_t *r=(pq_reader_t*)calloc(1,sizeof(*r));
    if(!r)return NULL;
    r->fp=fopen(path,"rb");
    if(!r->fp){free(r);return NULL;}
    uint8_t magic[4];
    if(fread(magic,1,4,r->fp)!=4||memcmp(magic,"PAR1",4)!=0){fclose(r->fp);free(r);return NULL;}
    if(read_footer(r)<0){pq_close(r);return NULL;}
    return r;
}

void pq_close(pq_reader_t *r)
{
    if(!r)return;
    if(r->fp)fclose(r->fp);
    if(r->cols){for(int32_t i=0;i<r->ncols;i++)free(r->cols[i].name);free(r->cols);}
    if(r->rgs){for(int32_t i=0;i<r->num_row_groups;i++)free(r->rgs[i].chunks);free(r->rgs);}
    free(r->mskql_types); free(r->phys_types); free(r);
}

int             pq_num_columns(const pq_reader_t *r)   {return r->ncols;}
int64_t         pq_num_rows(const pq_reader_t *r)      {return r->total_rows;}
int32_t         pq_num_row_groups(const pq_reader_t *r){return r->num_row_groups;}
const char     *pq_column_name(const pq_reader_t *r,int c)    {return r->cols[c].name;}
enum column_type pq_column_type(const pq_reader_t *r,int c)   {return r->mskql_types[c];}
enum pq_phys_type pq_column_phys_type(const pq_reader_t *r,int c){return r->phys_types[c];}

/* ---- Decompression ---- */

static int decompress_page(int32_t codec, const uint8_t *src, size_t slen,
                            uint8_t *dst, size_t dcap, size_t *dlen)
{
    switch(codec){
    case PQF_COMPRESS_UNCOMPRESSED:
        if(slen>dcap)return -1; memcpy(dst,src,slen); *dlen=slen; return 0;
    case PQF_COMPRESS_SNAPPY:
        return pq_snappy_decompress(src,slen,dst,dcap,dlen);
    case PQF_COMPRESS_ZSTD:{
        size_t r=ZSTD_decompress(dst,dcap,src,slen);
        if(ZSTD_isError(r))return -1; *dlen=r; return 0;}
    case PQF_COMPRESS_GZIP:{
        uLongf ul=(uLongf)dcap;
        if(uncompress(dst,&ul,src,(uLong)slen)!=Z_OK)return -1;
        *dlen=(size_t)ul; return 0;}
    default: return -1;
    }
}

/* ---- RLE/bit-packed decoder ---- */

static inline uint32_t rd_le32(const uint8_t *p){
    return (uint32_t)p[0]|((uint32_t)p[1]<<8)|((uint32_t)p[2]<<16)|((uint32_t)p[3]<<24);
}

static int rle_decode(const uint8_t *src, size_t slen, int bw, int32_t *out, int64_t count)
{
    if(bw==0){memset(out,0,(size_t)count*sizeof(int32_t));return 0;}
    const uint8_t *p=src,*end=src+slen; int64_t filled=0;
    while(filled<count&&p<end){
        uint32_t hdr=0;int sh=0;
        while(p<end){uint8_t b=*p++;hdr|=(uint32_t)(b&0x7f)<<sh;if(!(b&0x80))break;sh+=7;}
        if(hdr&1){ /* bit-packed: (hdr>>1) groups of 8 */
            int64_t grps=(int64_t)(hdr>>1),vals=grps*8;
            int64_t bytes=(grps*8*bw+7)/8;
            for(int64_t i=0;i<vals&&filled<count;i++){
                int64_t bp=i*bw; int64_t by=bp/8; int bo=(int)(bp%8);
                uint32_t raw=0;
                for(int k=0;k<4&&p+by+k<end;k++) raw|=(uint32_t)p[by+k]<<(8*k);
                out[filled++]=(int32_t)((raw>>bo)&((1u<<bw)-1));
            }
            p+=bytes;
        }else{ /* RLE */
            int64_t reps=(int64_t)(hdr>>1); int byt=(bw+7)/8;
            if(p+byt>end)return -1;
            uint32_t val=0; for(int b=0;b<byt;b++) val|=(uint32_t)p[b]<<(8*b); p+=byt;
            for(int64_t i=0;i<reps&&filled<count;i++) out[filled++]=(int32_t)val;
        }
    }
    return 0;
}

/* ---- Page header parser ---- */

struct phdr {
    int32_t type,usize,csize,nvals,enc,def_enc,rep_enc,dict_nvals;
    int32_t v2_def_bytes,v2_rep_bytes,v2_is_compressed;
};

static int parse_phdr(TB *t, struct phdr *ph)
{
    memset(ph,0,sizeof(*ph)); ph->v2_is_compressed=1;
    int16_t prev=0;
    while(t->pos<t->len){
        int ft;int16_t fid;if(tb_fhdr(t,&ft,&fid,prev)<0)return -1;if(!ft)break;prev=fid;
        switch(fid){
        case 1:{int32_t v;if(tb_i32(t,&v)<0)return -1;ph->type=v;break;}
        case 2:{int32_t v;if(tb_i32(t,&v)<0)return -1;ph->usize=v;break;}
        case 3:{int32_t v;if(tb_i32(t,&v)<0)return -1;ph->csize=v;break;}
        case 4:{int64_t v;if(tb_i64(t,&v)<0)return -1;(void)v;break;} /* crc */
        case 5:{ /* DataPageHeader */
            int16_t p2=0;
            while(t->pos<t->len){int dft;int16_t dfid;if(tb_fhdr(t,&dft,&dfid,p2)<0)return -1;if(!dft)break;p2=dfid;
                switch(dfid){
                case 1:{int32_t v;if(tb_i32(t,&v)<0)return -1;ph->nvals=v;break;}
                case 2:{int32_t v;if(tb_i32(t,&v)<0)return -1;ph->enc=v;break;}
                case 3:{int32_t v;if(tb_i32(t,&v)<0)return -1;ph->def_enc=v;break;}
                case 4:{int32_t v;if(tb_i32(t,&v)<0)return -1;ph->rep_enc=v;break;}
                default:if(tb_skip(t,dft)<0)return -1;break;}}
            break;}
        case 7:{ /* DictionaryPageHeader */
            int16_t p2=0;
            while(t->pos<t->len){int dft;int16_t dfid;if(tb_fhdr(t,&dft,&dfid,p2)<0)return -1;if(!dft)break;p2=dfid;
                switch(dfid){
                case 1:{int32_t v;if(tb_i32(t,&v)<0)return -1;ph->dict_nvals=v;break;}
                default:if(tb_skip(t,dft)<0)return -1;break;}}
            break;}
        case 8:{ /* DataPageHeaderV2 */
            int16_t p2=0;
            while(t->pos<t->len){int dft;int16_t dfid;if(tb_fhdr(t,&dft,&dfid,p2)<0)return -1;if(!dft)break;p2=dfid;
                switch(dfid){
                case 1:{int32_t v;if(tb_i32(t,&v)<0)return -1;ph->nvals=v;break;}
                case 4:{int32_t v;if(tb_i32(t,&v)<0)return -1;ph->enc=v;break;}
                case 5:{int32_t v;if(tb_i32(t,&v)<0)return -1;ph->v2_def_bytes=v;break;}
                case 6:{int32_t v;if(tb_i32(t,&v)<0)return -1;ph->v2_rep_bytes=v;break;}
                case 7:ph->v2_is_compressed=(ft==1)?1:0;break;
                default:if(tb_skip(t,dft)<0)return -1;break;}}
            ph->type=PQF_PAGE_DATA_V2; break;}
        default:if(tb_skip(t,ft)<0)return -1;break;
        }
    }
    return 0;
}

/* ---- Dictionary storage ---- */

struct dict {
    int32_t   count;
    uint8_t  *buf;
    uint32_t *lens; /* BYTE_ARRAY only */
    uint8_t **ptrs; /* BYTE_ARRAY only */
};

static void dict_free(struct dict *d){
    if(!d)return; free(d->buf); free(d->lens); free(d->ptrs);
}

/* Read and decode a dictionary page from file at `offset`. */
static int load_dict(FILE *fp, int64_t offset, int32_t codec, int32_t phys, struct dict *d)
{
    memset(d,0,sizeof(*d));
    if(fseek(fp,offset,SEEK_SET)<0)return -1;
    uint8_t hbuf[512]; size_t hr=fread(hbuf,1,sizeof(hbuf),fp);
    if(hr<4)return -1;
    TB ht={hbuf,0,hr}; struct phdr ph; if(parse_phdr(&ht,&ph)<0)return -1;
    if(ph.type!=PQF_PAGE_DICTIONARY)return -1;

    if(fseek(fp,offset+(int64_t)ht.pos,SEEK_SET)<0)return -1;
    uint8_t *cbuf=(uint8_t*)malloc((size_t)ph.csize); if(!cbuf)return -1;
    if((int32_t)fread(cbuf,1,(size_t)ph.csize,fp)!=ph.csize){free(cbuf);return -1;}
    uint8_t *ubuf=(uint8_t*)malloc((size_t)ph.usize+1); if(!ubuf){free(cbuf);return -1;}
    size_t ulen=0;
    if(decompress_page(codec,cbuf,(size_t)ph.csize,ubuf,(size_t)ph.usize+1,&ulen)<0){
        free(cbuf);free(ubuf);return -1;}
    free(cbuf);
    d->buf=ubuf; d->count=ph.dict_nvals;

    if(phys==PQF_PHYS_BYTE_ARRAY){
        d->lens=(uint32_t*)malloc((size_t)d->count*sizeof(uint32_t));
        d->ptrs=(uint8_t**)malloc((size_t)d->count*sizeof(uint8_t*));
        if(!d->lens||!d->ptrs)return -1;
        uint8_t *p=ubuf,*end=ubuf+ulen;
        for(int32_t i=0;i<d->count;i++){
            if(p+4>end)return -1;
            uint32_t l=rd_le32(p);p+=4;
            if(p+l>end)return -1;
            d->lens[i]=l; d->ptrs[i]=p; p+=l;
        }
    }
    return 0;
}

/* ---- Data page decoder ---- */

/* Decode def levels from V1 page data. Returns bytes consumed or -1. */
static int decode_def_v1(const uint8_t *data, size_t dlen, int32_t nvals,
                           uint8_t *nulls, int32_t *nvalid)
{
    if(dlen<4)return -1;
    uint32_t rlen=rd_le32(data);
    if(rlen+4>(uint32_t)dlen)return -1;
    int32_t *lvls=(int32_t*)malloc((size_t)nvals*sizeof(int32_t));
    if(!lvls)return -1;
    if(rle_decode(data+4,rlen,1,lvls,nvals)<0){free(lvls);return -1;}
    int32_t nv=0;
    for(int32_t i=0;i<nvals;i++){nulls[i]=(lvls[i]<1)?1:0;if(!nulls[i])nv++;}
    free(lvls); *nvalid=nv; return (int32_t)(rlen+4);
}

/* Decode one data page into col arrays starting at `base`.
 * Returns rows written (== nvals) or -1 on error. */
static int64_t decode_data_page(const struct phdr *ph, const uint8_t *data, size_t dlen,
                                 int32_t phys, int is_opt, const struct dict *d,
                                 void *col_data, uint8_t *col_nulls, char **col_strs,
                                 int64_t base, int64_t cap)
{
    int32_t nvals=ph->nvals;
    if(base+nvals>cap)return -1;
    uint8_t *nulls=col_nulls+base;
    int32_t nvalid=nvals;

    const uint8_t *payload=data; size_t plen=dlen;

    if(ph->type==PQF_PAGE_DATA_V2){
        if(is_opt&&ph->v2_def_bytes>0){
            if((size_t)ph->v2_def_bytes>plen)return -1;
            int32_t *lvls=(int32_t*)malloc((size_t)nvals*sizeof(int32_t));
            if(!lvls)return -1;
            if(rle_decode(payload,(size_t)ph->v2_def_bytes,1,lvls,nvals)<0){free(lvls);return -1;}
            nvalid=0;
            for(int32_t i=0;i<nvals;i++){nulls[i]=(lvls[i]<1)?1:0;if(!nulls[i])nvalid++;}
            free(lvls);
            payload+=ph->v2_def_bytes; plen-=(size_t)ph->v2_def_bytes;
        } else memset(nulls,0,(size_t)nvals);
        if(ph->v2_rep_bytes>0){if((size_t)ph->v2_rep_bytes>plen)return -1;payload+=ph->v2_rep_bytes;plen-=(size_t)ph->v2_rep_bytes;}
    } else {
        if(is_opt){
            int c=decode_def_v1(payload,plen,nvals,nulls,&nvalid);
            if(c<0)return -1;
            payload+=c; plen-=(size_t)c;
        } else memset(nulls,0,(size_t)nvals);
    }

    int enc=ph->enc;

    /* RLE_DICT / PLAIN_DICT */
    if(enc==PQF_ENC_RLE_DICT||enc==PQF_ENC_PLAIN_DICT){
        if(!d||!d->count)return -1;
        if(plen<1)return -1;
        int bw=(int)payload[0]; payload++; plen--;
        int32_t *idx=(int32_t*)malloc((size_t)nvalid*sizeof(int32_t));
        if(!idx)return -1;
        if(rle_decode(payload,plen,bw,idx,nvalid)<0){free(idx);return -1;}
        int32_t vi=0;
        for(int32_t i=0;i<nvals;i++){
            if(nulls[i])continue;
            int32_t id=idx[vi++];
            if(id<0||id>=d->count){free(idx);return -1;}
            switch(phys){
            case PQF_PHYS_BOOLEAN: ((uint8_t*)col_data)[base+i]=((uint8_t*)d->buf)[id]; break;
            case PQF_PHYS_INT32:   ((int32_t*)col_data)[base+i]=((int32_t*)d->buf)[id]; break;
            case PQF_PHYS_INT64:   ((int64_t*)col_data)[base+i]=((int64_t*)d->buf)[id]; break;
            case PQF_PHYS_FLOAT:   ((float*)  col_data)[base+i]=((float*)  d->buf)[id]; break;
            case PQF_PHYS_DOUBLE:  ((double*) col_data)[base+i]=((double*) d->buf)[id]; break;
            case PQF_PHYS_INT96:{
                const uint8_t *raw=d->buf+id*12;
                int64_t ns=0;memcpy(&ns,raw,8);int32_t jd=0;memcpy(&jd,raw+8,4);
                ((int64_t*)col_data)[base+i]=((int64_t)jd-2440588)*86400LL*1000000LL+ns/1000LL;break;}
            case PQF_PHYS_BYTE_ARRAY:
            case PQF_PHYS_FIXED_LEN_BYTE_ARRAY:{
                uint32_t sl=d->lens[id];char *s=(char*)malloc(sl+1);
                if(!s){free(idx);return -1;}
                memcpy(s,d->ptrs[id],sl);s[sl]='\0';col_strs[base+i]=s;break;}
            }
        }
        free(idx); return nvals;
    }

    /* PLAIN */
    if(enc==PQF_ENC_PLAIN){
        if(phys==PQF_PHYS_BYTE_ARRAY){
            const uint8_t *p=payload,*end=payload+plen;
            for(int32_t i=0;i<nvals;i++){
                if(nulls[i])continue;
                if(p+4>end)return -1;
                uint32_t sl=rd_le32(p);p+=4;
                if(p+sl>end)return -1;
                char *s=(char*)malloc(sl+1);if(!s)return -1;
                memcpy(s,p,sl);s[sl]='\0';col_strs[base+i]=s;p+=sl;
            }
        } else if(phys==PQF_PHYS_BOOLEAN){
            uint8_t *dst=(uint8_t*)col_data; int32_t vi2=0;
            for(int32_t i=0;i<nvals;i++){
                if(nulls[i])continue;
                int32_t by=vi2/8,bo=vi2%8;
                if(payload+by>=payload+plen)return -1;
                dst[base+i]=(payload[by]>>bo)&1; vi2++;
            }
        } else if(phys==PQF_PHYS_INT96){
            int64_t *dst=(int64_t*)col_data; const uint8_t *p=payload;
            for(int32_t i=0;i<nvals;i++){
                if(nulls[i])continue;
                if(p+12>payload+plen)return -1;
                int64_t ns=0;memcpy(&ns,p,8);int32_t jd=0;memcpy(&jd,p+8,4);
                dst[base+i]=((int64_t)jd-2440588)*86400LL*1000000LL+ns/1000LL; p+=12;
            }
        } else {
            size_t esz=0;
            switch(phys){
            case PQF_PHYS_INT32: case PQF_PHYS_FLOAT: esz=4; break;
            case PQF_PHYS_INT64: case PQF_PHYS_DOUBLE: esz=8; break;
            default: esz=0; break;
            }
            if(esz){
                const uint8_t *p=payload;
                for(int32_t i=0;i<nvals;i++){
                    if(nulls[i])continue;
                    if(p+esz>payload+plen)return -1;
                    memcpy((uint8_t*)col_data+(base+i)*esz,p,esz); p+=esz;
                }
            }
        }
        return nvals;
    }

    /* RLE (for BOOLEAN columns) */
    if(enc==PQF_ENC_RLE&&phys==PQF_PHYS_BOOLEAN){
        const uint8_t *rp=payload; size_t rl=plen;
        if(ph->type==PQF_PAGE_DATA){
            if(plen<4)return -1; uint32_t rlen=rd_le32(payload); rp=payload+4; rl=rlen;
        }
        int32_t *tmp=(int32_t*)malloc((size_t)nvalid*sizeof(int32_t));if(!tmp)return -1;
        if(rle_decode(rp,rl,1,tmp,nvalid)<0){free(tmp);return -1;}
        uint8_t *dst=(uint8_t*)col_data; int32_t vi2=0;
        for(int32_t i=0;i<nvals;i++){if(nulls[i])continue;dst[base+i]=(uint8_t)tmp[vi2++];}
        free(tmp); return nvals;
    }

    return -1; /* unsupported encoding */
}

/* ---- pq_read_row_group — public ---- */

int pq_read_row_group(pq_reader_t *r, int rg_idx, struct pq_column *cols)
{
    if(rg_idx<0||rg_idx>=r->num_row_groups)return -1;
    struct pq_row_group_meta *rg=&r->rgs[rg_idx];
    int64_t nrows=rg->num_rows;

    /* Allocate output */
    for(int c=0;c<r->ncols;c++){
        cols[c].nrows=nrows; cols[c].phys=r->phys_types[c];
        cols[c].nulls=(uint8_t*)calloc((size_t)nrows,1);
        size_t esz=0;
        switch(r->phys_types[c]){
        case PQ_PHYS_BOOLEAN:              esz=1; break;
        case PQ_PHYS_INT32: case PQ_PHYS_FLOAT: esz=4; break;
        case PQ_PHYS_INT64: case PQ_PHYS_DOUBLE: case PQ_PHYS_INT96: esz=8; break;
        case PQ_PHYS_BYTE_ARRAY: case PQ_PHYS_FIXED_LEN_BYTE_ARRAY: esz=sizeof(char*); break;
        }
        cols[c].data=calloc((size_t)nrows,esz?esz:1);
        if(!cols[c].nulls||!cols[c].data){
            for(int cc=0;cc<=c;cc++){free(cols[cc].data);cols[cc].data=NULL;free(cols[cc].nulls);cols[cc].nulls=NULL;}
            return -1;
        }
    }

    /* Decode each column */
    for(int c=0;c<r->ncols;c++){
        struct pq_chunk_meta *ck=&rg->chunks[c];
        int32_t phys=r->cols[c].phys_type;
        int is_opt=r->cols[c].is_optional;
        int32_t codec=ck->compression;

        /* Load dictionary page if present */
        struct dict dict; memset(&dict,0,sizeof(dict)); int has_dict=0;
        if(ck->dict_page_offset>0){
            if(load_dict(r->fp,ck->dict_page_offset,codec,phys,&dict)>=0) has_dict=1;
        }

        /* Read all data pages for this column chunk */
        int64_t base=0;
        int64_t data_start=ck->data_page_offset;
        int64_t data_end=data_start+ck->total_compressed_size;
        if(has_dict&&ck->dict_page_offset<=data_start) data_end=data_start+ck->total_compressed_size;

        if(fseek(r->fp,data_start,SEEK_SET)<0){dict_free(&dict);continue;}

        while(base<nrows){
            long cur=ftell(r->fp);
            if(cur<0||(int64_t)cur>=data_end)break;

            /* Read page header */
            uint8_t hbuf[512]; size_t hr=fread(hbuf,1,sizeof(hbuf),r->fp);
            if(hr<4)break;
            TB ht={hbuf,0,hr}; struct phdr ph; if(parse_phdr(&ht,&ph)<0)break;
            if(ph.type==PQF_PAGE_DICTIONARY){
                /* skip dict page if encountered here */
                if(fseek(r->fp,cur+(long)ht.pos+(long)ph.csize,SEEK_SET)<0)break;
                continue;
            }

            /* Seek to compressed data */
            if(fseek(r->fp,cur+(long)ht.pos,SEEK_SET)<0)break;
            uint8_t *cbuf=(uint8_t*)malloc((size_t)ph.csize); if(!cbuf)break;
            if((int32_t)fread(cbuf,1,(size_t)ph.csize,r->fp)!=ph.csize){free(cbuf);break;}

            /* For V2: levels are uncompressed, only data portion is compressed */
            size_t uncomp_data_off=0;
            if(ph.type==PQF_PAGE_DATA_V2&&!ph.v2_is_compressed){
                /* entire payload is uncompressed */
                uint8_t *ubuf=(uint8_t*)malloc((size_t)ph.usize+1); if(!ubuf){free(cbuf);break;}
                memcpy(ubuf,cbuf,(size_t)ph.csize); ubuf[ph.csize]='\0';
                int64_t rows=decode_data_page(&ph,ubuf,(size_t)ph.usize,
                                               phys,is_opt,has_dict?&dict:NULL,
                                               cols[c].data,cols[c].nulls,(char**)cols[c].data,
                                               base,nrows);
                if(cols[c].phys!=PQ_PHYS_BYTE_ARRAY&&cols[c].phys!=PQ_PHYS_FIXED_LEN_BYTE_ARRAY)
                    (void)rows;
                free(ubuf); free(cbuf);
                if(rows<0)break; base+=rows; continue;
            }

            /* Decompress */
            size_t levels_prefix=0;
            if(ph.type==PQF_PAGE_DATA_V2){
                levels_prefix=(size_t)(ph.v2_def_bytes+ph.v2_rep_bytes);
            }
            uint8_t *ubuf=(uint8_t*)malloc((size_t)ph.usize+1); if(!ubuf){free(cbuf);break;}
            /* V2: levels uncompressed, rest compressed */
            int decomp_ok=0;
            if(ph.type==PQF_PAGE_DATA_V2&&levels_prefix>0){
                memcpy(ubuf,cbuf,levels_prefix);
                size_t dlen=0;
                if(decompress_page(codec,cbuf+levels_prefix,(size_t)ph.csize-levels_prefix,
                                    ubuf+levels_prefix,(size_t)ph.usize-levels_prefix+1,&dlen)>=0)
                    decomp_ok=1;
            } else {
                size_t dlen=0;
                if(decompress_page(codec,cbuf,(size_t)ph.csize,ubuf,(size_t)ph.usize+1,&dlen)>=0)
                    decomp_ok=1;
            }
            free(cbuf);
            if(!decomp_ok){free(ubuf);break;}

            /* Determine str array pointer */
            char **strs=(ph.type!=PQF_PAGE_DICTIONARY&&
                         (phys==PQF_PHYS_BYTE_ARRAY||phys==PQF_PHYS_FIXED_LEN_BYTE_ARRAY))
                         ? (char**)cols[c].data : NULL;

            int64_t rows=decode_data_page(&ph,ubuf,(size_t)ph.usize,
                                           phys,is_opt,has_dict?&dict:NULL,
                                           cols[c].data,cols[c].nulls,strs,
                                           base,nrows);
            free(ubuf);
            if(rows<0) break;
            base+=rows;
            (void)uncomp_data_off;
        }

        dict_free(&dict);
    }
    return 0;
}

#endif /* MSKQL_WASM */
