#include "sqlc.h"

#include <stddef.h> /* for NULL */

#include <android/log.h>

#include "sqlite3.h"

//#include "jsmn.h"

#include <stdbool.h>

#define BASE_HANDLE_OFFSET 0x100000000LL

#ifdef SQLC_KEEP_ANDROID_LOG
// ref: http://www.ibm.com/developerworks/opensource/tutorials/os-androidndk/index.html
#define MYLOG(...) __android_log_print(ANDROID_LOG_VERBOSE, "sqlg", __VA_ARGS__)
#else
#define MYLOG(...) ;
#endif

#define HANDLE_FROM_VP(p) ( BASE_HANDLE_OFFSET + ( (unsigned char *)(p) - (unsigned char *)NULL ) )
#define HANDLE_TO_VP(h) (void *)( (unsigned char *)NULL + (ptrdiff_t)((h) - BASE_HANDLE_OFFSET) )

int sqlc_api_version_check(int sqlc_api_version)
{
  return (sqlc_api_version != SQLC_API_VERSION) ? SQLC_RESULT_ERROR : SQLC_RESULT_OK;
}

sqlc_handle_t sqlc_api_db_open(int sqlc_api_version, const char *filename, int flags)
{
  if (sqlc_api_version != SQLC_API_VERSION) return SQLC_RESULT_ERROR;

  return sqlc_db_open(filename, flags);
}

sqlc_handle_t sqlc_db_open(const char *filename, int flags)
{
  sqlite3 *d1;
  int r1;

  MYLOG("db_open %s %d", filename, flags);

  r1 = sqlite3_open_v2(filename, &d1, flags, NULL);

  MYLOG("db_open %s result %d ptr %p", filename, r1, d1);

  return (r1 == 0) ? HANDLE_FROM_VP(d1) : -r1;
}

sqlc_handle_t sqlc_db_prepare_st(sqlc_handle_t db, const char *sql)
{
  sqlite3 *mydb = HANDLE_TO_VP(db);
  sqlite3_stmt *s;
  int rv;

  MYLOG("prepare db %p sql %s", mydb, sql);

  rv = sqlite3_prepare_v2(mydb, sql, -1, &s, NULL);

  return (rv == 0) ? HANDLE_FROM_VP(s) : -rv;
}

/** FUTURE TBD (???) for sqlcipher:
int sqlc_db_key_bytes(sqlc_handle_t db, unsigned char *key_bytes, int num_bytes)
{
  sqlite3 *mydb = HANDLE_TO_VP(db);

#ifdef SQLITE_HAS_CODEC
  return sqlite3_key(mydb, key_bytes, num_bytes);
#else
  return SQLITE_ERROR;
#endif
}

int sqlc_db_rekey_bytes(sqlc_handle_t db, unsigned char *key_bytes, int num_bytes)
{
  sqlite3 *mydb = HANDLE_TO_VP(db);

#ifdef SQLITE_HAS_CODEC
  return sqlite3_rekey(mydb, key_bytes, num_bytes);
#else
  return SQLITE_ERROR;
#endif
}
**/

int sqlc_db_key_native_string(sqlc_handle_t db, char *key_string)
{
  // NOT IMPLEMENTED in this version branch:
  return SQLITE_INTERNAL;
}

sqlc_long_t sqlc_db_last_insert_rowid(sqlc_handle_t db)
{
  sqlite3 *mydb = HANDLE_TO_VP(db);

  return sqlite3_last_insert_rowid(mydb);
}

int sqlc_db_total_changes(sqlc_handle_t db)
{
  sqlite3 *mydb = HANDLE_TO_VP(db);

  return sqlite3_total_changes(mydb);
}

int sqlc_db_errcode(sqlc_handle_t db)
{
  sqlite3 *mydb = HANDLE_TO_VP(db);

  return sqlite3_errcode(mydb);
}

const char * sqlc_db_errmsg_native(sqlc_handle_t db)
{
  sqlite3 *mydb = HANDLE_TO_VP(db);

  return sqlite3_errmsg(mydb);
}

const char * sqlc_errstr_native(int errcode)
{
  return sqlite3_errstr(errcode);
}

int sqlc_st_bind_double(sqlc_handle_t st, int pos, double val)
{
  sqlite3_stmt *myst = HANDLE_TO_VP(st);

  MYLOG("%s %p %d %lf", __func__, myst, pos, val);

  return sqlite3_bind_double(myst, pos, val);
}

int sqlc_st_bind_int(sqlc_handle_t st, int pos, int val)
{
  sqlite3_stmt *myst = HANDLE_TO_VP(st);

  MYLOG("%s %p %d %d", __func__, myst, pos, val);

  return sqlite3_bind_int(myst, pos, val);
}

int sqlc_st_bind_long(sqlc_handle_t st, int pos, sqlc_long_t val)
{
  sqlite3_stmt *myst = HANDLE_TO_VP(st);

  MYLOG("%s %p %d %lld", __func__, myst, pos, val);

  return sqlite3_bind_int64(myst, pos, val);
}

int sqlc_st_bind_null(sqlc_handle_t st, int pos)
{
  sqlite3_stmt *myst = HANDLE_TO_VP(st);
  return sqlite3_bind_null(myst, pos);
}

int sqlc_st_bind_text_native(sqlc_handle_t st, int col, const char *val)
{
  sqlite3_stmt *myst = HANDLE_TO_VP(st);

  MYLOG("%s %p %d %s", __func__, myst, col, val);

  return sqlite3_bind_text(myst, col, val, -1, SQLITE_TRANSIENT);
}

int sqlc_st_step(sqlc_handle_t stmt)
{
  sqlite3_stmt *mystmt = HANDLE_TO_VP(stmt);

  return sqlite3_step(mystmt);
}

int sqlc_st_column_count(sqlc_handle_t st)
{
  sqlite3_stmt *myst = HANDLE_TO_VP(st);

  MYLOG("%s %p", __func__, myst);

  return sqlite3_column_count(myst);
}

const char *sqlc_st_column_name(sqlc_handle_t st, int col)
{
  sqlite3_stmt *myst = HANDLE_TO_VP(st);

  MYLOG("%s %p %d", __func__, myst, col);

  return sqlite3_column_name(myst, col);
}

double sqlc_st_column_double(sqlc_handle_t st, int col)
{
  return sqlite3_column_double(HANDLE_TO_VP(st), col);
}

int sqlc_st_column_int(sqlc_handle_t st, int col)
{
  return sqlite3_column_int(HANDLE_TO_VP(st), col);
}

sqlc_long_t sqlc_st_column_long(sqlc_handle_t st, int col)
{
  return sqlite3_column_int64(HANDLE_TO_VP(st), col);
}

const char *sqlc_st_column_text_native(sqlc_handle_t st, int col)
{
  sqlite3_stmt *myst = HANDLE_TO_VP(st);

  MYLOG("%s %p %d", __func__, myst, col);

  return sqlite3_column_text(myst, col);
}

int sqlc_st_column_type(sqlc_handle_t st, int col)
{
  sqlite3_stmt *myst = HANDLE_TO_VP(st);

  MYLOG("%s %p %d", __func__, myst, col);

  return sqlite3_column_type(myst, col);
}

int sqlc_st_finish(sqlc_handle_t st)
{
  sqlite3_stmt *myst = HANDLE_TO_VP(st);

  MYLOG("%s %p", __func__, myst);

  return sqlite3_finalize(myst);
}

int sqlc_db_close(sqlc_handle_t db)
{
  sqlite3 *mydb = HANDLE_TO_VP(db);

  MYLOG("%s %p", __func__, mydb);

// XXX TBD consider sqlite3_close() vs sqlite3_close_v2() ??:
  return sqlite3_close(mydb);
}

struct fj_s {
  sqlite3 * mydb;
  void * cleanup1;
  void * cleanup2;
};

sqlc_handle_t sqlc_db_new_fj(sqlc_handle_t db)
{
  sqlite3 *mydb = HANDLE_TO_VP(db);

  struct fj_s * myfj = malloc(sizeof(struct fj_s));
  myfj->mydb = mydb;
  myfj->cleanup1 = NULL;
  myfj->cleanup2 = NULL;

  return HANDLE_FROM_VP(myfj);
}

void sqlc_fj_dispose(sqlc_handle_t fj)
{
  struct fj_s * myfj = HANDLE_TO_VP(fj);
  free(myfj->cleanup1);
  free(myfj->cleanup2);
  free(myfj);
}

int sj(const char * j, int tl, char * a)
{
  int ti=0;
  int ai=0;
  while (ti<tl) {
    char c = j[ti];
    if (c == '\\') {
      // XXX TODO TODO TODO TODO TODO
      switch(j[ti+1]) {
      case '\"':
        a[ai++] = '\"';
        ti += 2;
        break;

      case '\\':
        a[ai++] = '\\';
        ti += 2;
        break;

      case 'r':
        a[ai++] = '\r';
        ti += 2;
        break;

      case 'n':
        a[ai++] = '\n';
        ti += 2;
        break;

      case 't':
        //a[ai++] = '\t';
        //sprintf(a+ai, ".tab.");
        //ai += strlen(a+ai);
        a[ai++] = '\t';
        ti += 2;
        break;

      default:
        // XXX TODO what to do??
        ti += 2;
        break;
      }
    } else if (c >= 0xe0) {
      //sprintf(a+ai, ".%02x.%02x.%02x.", c, j[ti+1], j[ti+2]);
      //ai += strlen(a+ai);
      //ti += 3;
      a[ai++]=j[ti++];
      a[ai++]=j[ti++];
      a[ai++]=j[ti++];
    } else if (c >= 0xc0) {
      //sprintf(a+ai, ".%02x.%02x.", c, j[ti+1]);
      //ai += strlen(a+ai);
      //ti += 2;
      a[ai++]=j[ti++];
      a[ai++]=j[ti++];
    } else if (c >= 128) {
      sprintf(a+ai, "-%02x-", c);
      ai += strlen(a+ai);
      ti += 1;
    } else {
      a[ai++]=j[ti++];
    }
  }

  return ai;
}

const char *sqlc_fj_run(sqlc_handle_t fj, const char *batch_json, int ll)
{
// XXX MAJOR TODO(s)
// handle constraint violation
// extra json char issue(s)
// basic optimization(s)
//
// DOUBLE-CHECK:
// deal with json char issues
// check return values
// error handling
// bind args
// deal with numbers, true/false, null, etc. etc. etc.
// rowsAffected/insertId
// opt keep track of result json position
// double-check for possible memory overflows

  struct fj_s * myfj = HANDLE_TO_VP(fj);
  sqlite3 *mydb = myfj->mydb;

  //jsmn_parser myparser;
  //jsmntok_t *tokn = NULL;
  //int r = -1;

  int cs;

  //int as = 0;
  int flen = 0;
  char nf[22];
  int nflen = 0;

  int fi = 0;
  sqlite3_stmt *s = NULL;
  int rv = -1;
  int jj, cc;
  int param_count = 0;
  int bi = 0;

  // FUTURE TBD optimize?
  // For alloc memory test:
  //const FIRST_ALLOC = 40;
  // Desired for normal situations:
  //const FIRST_ALLOC = 1000;
  // XXX TBD NEEDED to pass big memory test:
  const FIRST_ALLOC = 1000000;
  const NEXT_ALLOC = 80; // extra extra padding extra extra padding

  char * rr;
  int rrlen = 0;
  int arlen = 0;
  const char * pptext = 0;
  int pplen = 0;

// Double alloc every time:
#define EXTRA_ALLOC rrlen
// Don't double alloc every time (for extra alloc memory test):
//#define EXTRA_ALLOC 11

  free(myfj->cleanup1);
  myfj->cleanup1 = NULL;
  free(myfj->cleanup2);
  myfj->cleanup2 = NULL;

  //tokn = tokns;
  //myfj->cleanup1 = tokn = malloc(ll*sizeof(jsmntok_t));

  //jsmn_init(&myparser);
  //r = jsmn_parse(&myparser, batch_json, strlen(batch_json), tokn, ll);
  //if (r < 0) return "{\"message\": \"jsmn_parse error 1\"}";

  // FUTURE TBD check for tok overflow?
  //if (i >= r) return "{\"message\": \"memory error 1\"}";

  //if (tokn->type != JSMN_ARRAY) return "{\"message\": \"missing array 1\"}";
  //as = tokn->size;
  //++tokn;

#if 0
  if (tokn->type != JSMN_OBJECT) return "{\"message\": \"missing object 1\"}";
  ++tokn;

  // dbargs (ignored)
  if (tokn->type != JSMN_STRING) return "{\"message\": \"type error 1\"}";
  if (strncmp(batch_json+tokn->start, "dbargs", 6) != 0)
    return "{\"message\": \"arg error 1\"}";
  ++tokn;

  if (tokn->type != JSMN_OBJECT) return "{\"message\": \"type error 2\"}";
  tokn = tokn + (tokn->size << 1);
  ++tokn;

  // flen
  if (tokn->type != JSMN_STRING) return "{\"message\": \"type error 3\"}";
  if (strncmp(batch_json+tokn->start, "flen", 4) != 0)
    return "{\"message\": \"arg error 2\"}";
  ++tokn;
  if (tokn->type != JSMN_PRIMITIVE) return "{\"message\": \"type error 4\"}";
  nflen = tokn->end-tokn->start;
  strncpy(nf, batch_json+tokn->start, nflen);
  nf[nflen] = '\0';
  flen = atoi(nf);
  ++tokn;

  // flatlist
  if (tokn->type != JSMN_STRING) return "{\"message\": \"type error 5\"}";
  if (strncmp(batch_json+tokn->start, "flatlist", 8) != 0)
    return "{\"message\": \"arg error 3\"}";
  ++tokn;
  if (tokn->type != JSMN_ARRAY) return "{\"message\": \"type error 6\"}";
  // TODO get flatlist length
  ++tokn;
#endif

  cs=1;

#if 1
  // dbid
  //if (tokn->type != JSMN_PRIMITIVE) return "{\"message\": \"type error 4\"}";
  //++tokn;
  nflen=0;
  while(batch_json[cs+nflen] != ',') ++nflen;
  cs+=nflen+1;

  // flen (batch length)
  //if (tokn->type != JSMN_PRIMITIVE) return "{\"message\": \"type error 4a\"}";
  //nflen = tokn->end-tokn->start;
  nflen=0;
  while(batch_json[cs+nflen] != ',') ++nflen;
  //strncpy(nf, batch_json+tokn->start, nflen);
  strncpy(nf, batch_json+cs, nflen);
  nf[nflen] = '\0';
  flen = atoi(nf);
  //++tokn;
  cs+=nflen+1;
#endif

  // XXX TODO CHECK IN COFFEESCRIPT:
  // not needed here:
  // "check" first SQL:
  //if (tokn->type != JSMN_STRING) return "{\"message\": \"type error 7\"}";
  if (batch_json[cs] != '\"') return "{\"message\": \"type error 7\"}";

  myfj->cleanup2 = rr = malloc(arlen = FIRST_ALLOC);
  strcpy(rr, "[");
  rrlen = 1;

  for (fi=0; fi<flen; ++fi) {
    char cc;
    int ce;
    int tc0 = sqlite3_total_changes(mydb);

    //if (tokn->type != JSMN_STRING) return "{\"message\": \"type error (sql)\"}";
    //if (tokn->type != JSMN_STRING && tokn->type != JSMN_PRIMITIVE) return "{\"message\": \"type error (sql)\"}";
    //if (tokn->type != JSMN_STRING) return batch_json+tokn->start;
    if (batch_json[cs] != '\"') return batch_json+cs;
    ++cs;

    ce=cs;
    while ((cc=batch_json[ce]) != '\"') {
      if (cc == '\\') {
        ce += 2;
      } else if (cc >= 0xe0) {
        ce += 3;
      } else if (cc >= 0xc0) {
        ce += 2;
      } else {
        ++ce;
      }
    }
/*
{ // leaky test
char * a = strdup(batch_json);
++a[cs];
--a[ce-1];
return a;
}
*/

    //rv = sqlite3_prepare_v2(mydb, batch_json+tokn->start, tokn->end-tokn->start, &s, NULL);
    {
      // XXX FUTURE TBD keep buffer & free at the end
      //int te = tokn->end;
      //int tl = tokn->end-tokn->start;
      int tl = ce-cs;
      //char * a = malloc(tl+10); // extra padding
      char * a = malloc((tl<<3)+10); // extra padding
      //int ai = sj(batch_json+tokn->start, tl, a);
      int ai = sj(batch_json+cs, tl, a);
      rv = sqlite3_prepare_v2(mydb, a, ai, &s, NULL);
      free(a);
    }
    //++tokn;
    cs=ce+2;
    // TODO check rv
/*
{ // leaky test
char * b = strdup(batch_json);
b[cs-4]+=3;
return b;
}
*/

    // TODO deal with bind count
    //if (tokn->type != JSMN_PRIMITIVE) return "{\"message\": \"xxxx\"}";
    //if (tokn->type != JSMN_PRIMITIVE) return batch_json+tokn->start;
    //if (tokn->type != JSMN_PRIMITIVE) return "{\"message\": \"xxxx\"}";
    if (batch_json[cs] < '0' || batch_json[cs] > '9') return "{\"message\": \"xxxx\"}";
    //nflen = tokn->end-tokn->start;
    nflen=0;
    while(batch_json[cs+nflen] != ',') ++nflen;
    //strncpy(nf, batch_json+tokn->start, nflen);
    strncpy(nf, batch_json+cs, nflen);
    nf[nflen] = '\0';
    param_count = atoi(nf);
    //++tokn;
    cs+=nflen+1;
/*
if (fi != 0)
{ // leaky test
char * b = strdup(batch_json);
b[cs-2]+=3;
return b;
}
//*/

    //tokn = tokn + param_count;// XXX TEMP TEST

    // XXX TODO OVERFLOW ETC ETC

    if (rv != SQLITE_OK) {
#if 0 // XXX TODO
      tokn = tokn + param_count;
#endif
    } else {
//#if 0 // XXX TODO
      for (bi=1; bi<=param_count; ++bi) {
        // XXX TODO deal with BLOB etc etc
        // XXX TBD/TODO check bind result??
        if (batch_json[cs] != '\"') {
          // XXX TODO double-check:
          // XXX TODO TEST ALL:
          if (batch_json[cs] == 'n') {
            sqlite3_bind_null(s, bi);
          } else if (batch_json[cs] == 't') {
            sqlite3_bind_int(s, bi, 1);
          } else if (batch_json[cs] == 'f') {
            sqlite3_bind_int(s, bi, 0);
          } else {
            bool f=false;
            //int iii;

            //for (iii=tokn->start; iii!=tokn->end; ++iii) {
            //  if (batch_json[iii]=='.') {
            //    f = true;
            //    break;
            //  }
            //}
            ce=cs;
            while (batch_json[ce] != ',') {
              if (batch_json[ce] == '.') f=true;
              ++ce;
            }
/* **
{ // leaky test
char * b = strdup(batch_json);
++b[cs];
b[ce-1]+=3;
return b;
}
// */

            nflen = ce-cs;
            strncpy(nf, batch_json+cs, nflen);
            nf[nflen] = '\0';

            if (f) {
              sqlite3_bind_double(s, bi, atof(nf));
            } else {
              sqlite3_bind_int64(s, bi, atoll(nf));
            }
            cs=ce+1;
          }
          while (batch_json[cs] != ',') ++cs;
          ++cs;
        } else {
          // XXX FUTURE TBD keep buffer & free at the end
          //sqlite3_bind_text(s, bi, batch_json+tokn->start, tokn->end-tokn->start, SQLITE_TRANSIENT);

          ce=++cs;
          while ((cc=batch_json[ce]) != '\"') {
            if (cc == '\\') {
              ce += 2;
            } else if (cc >= 0xe0) {
              ce += 3;
            } else if (cc >= 0xc0) {
              ce += 2;
            } else {
              ++ce;
            }
          }
/* **
{ // leaky test
char * b = strdup(batch_json);
++b[cs];
--b[ce-1];
return b;
}
// */
          //int te = tokn->end;
          //int tl = tokn->end-tokn->start;
          int tl = ce-cs;
          //char * a = malloc(tl+10); // extra padding
          {
          char * a = malloc((tl<<3)+10); // extra padding
          //int ai = sj(batch_json+tokn->start, tl, a);
          int ai = sj(batch_json+cs, tl, a);
          sqlite3_bind_text(s, bi, a, ai, SQLITE_TRANSIENT);
          free(a);
          }
          cs=ce+2;
/* **
{ // leaky test
char * b = strdup(batch_json);
b[cs-2]+=3;
return b;
}
// */
        }
        //++tokn;
      }
//#endif

      rv=sqlite3_step(s);
      if (rv == SQLITE_ROW) {
        strcpy(rr+rrlen, "\"okrows\",");
        rrlen += 9;
        do {
          cc = sqlite3_column_count(s);
          sprintf(nf, "%d", cc);
          strcpy(rr+rrlen, nf);
          rrlen += strlen(nf);
          strcpy(rr+rrlen, ",");
          ++rrlen;

          for (jj=0; jj<cc; ++jj) {
            int ct = sqlite3_column_type(s, jj);

            strcpy(rr+rrlen, "\"");
            rrlen += 1;
            pptext = sqlite3_column_name(s, jj);
            pplen = strlen(pptext);
            if (rrlen + pplen + NEXT_ALLOC > arlen) {
              arlen += EXTRA_ALLOC + pplen + NEXT_ALLOC;
              myfj->cleanup2 = rr = realloc(rr, arlen);
            }
            strcpy(rr+rrlen, pptext);
            rrlen += pplen;
            strcpy(rr+rrlen, "\",");
            rrlen += 2;

            if (ct == SQLITE_NULL) {
              // XXX TODO TEST ME
              strcpy(rr+rrlen, "null,");
              rrlen += 5;
            } else {
              pptext = sqlite3_column_text(s, jj);
              pplen = strlen(pptext);
              //pplen = 0;
              //while(pptext[pplen] != 0) ++pplen;

              if (rrlen + pplen + NEXT_ALLOC > arlen) {
                arlen += EXTRA_ALLOC + pplen + NEXT_ALLOC;
                myfj->cleanup2 = rr = realloc(rr, arlen);
              }

              if (ct == SQLITE_INTEGER || ct == SQLITE_FLOAT) {
                strcpy(rr+rrlen, pptext);
                rrlen += pplen;
                strcpy(rr+rrlen, ",");
                rrlen += 1;
              } else {
                int pi=0;
                // XXX FUTURE TODO handle BLOB correctly
                // XXX TODO CONVERT TO PROPER JSON !! !! !!
                strcpy(rr+rrlen, "\"");
                rrlen += 1;
                //strcpy(rr+rrlen, pptext);
                //rrlen += pplen;
                while (pi < pplen) {
                  int pc = pptext[pi];

                  if (pc == '\\') {
                    rr[rrlen++] = '\\';
                    rr[rrlen++] = '\\';
                    pi += 1;
                  } else if (pc == '\"') {
                    rr[rrlen++] = '\\';
                    rr[rrlen++] = '\"';
                    pi += 1;
                  } else if (pc >= 32 && pc < 127) {
                    rr[rrlen++] = pptext[pi++];
                  } else if (pc > 0xe0) {
                    //sprintf(rr+rrlen, ".%02x.%02x.%02x.", pc, pptext[pi+1], pptext[pi+2]);
                    //rrlen += strlen(rr+rrlen);
                    //pi += 3;
                    rr[rrlen++] = pptext[pi++];
                    rr[rrlen++] = pptext[pi++];
                    rr[rrlen++] = pptext[pi++];
                  } else if (pc >= 0xc0) {
                    //sprintf(rr+rrlen, ".%02x.%02x.", pc, pptext[pi+1]);
                    //rrlen += strlen(rr+rrlen);
                    //pi += 2;
                    rr[rrlen++] = pptext[pi++];
                    rr[rrlen++] = pptext[pi++];
                  } else if (pc >= 128) {
                    // XXX TBD ???:
                    sprintf(rr+rrlen, "-%02x-", pc);
                    rrlen += strlen(rr+rrlen);
                    pi += 1;
                  } else if (pc == '\t') {
                    rr[rrlen++] = '\\';
                    rr[rrlen++] = 't';
                    pi += 1;
                  } else if (pc == '\r') {
                    rr[rrlen++] = '\\';
                    rr[rrlen++] = 'r';
                    pi += 1;
                  } else if (pc == '\n') {
                    rr[rrlen++] = '\\';
                    rr[rrlen++] = 'n';
                    pi += 1;
                  } else {
                    sprintf(rr+rrlen, "?%02x?", pc);
                    rrlen += strlen(rr+rrlen);
                    pi += 1;
                  }
                }
                strcpy(rr+rrlen, "\",");
                rrlen += 2;
              }
            }
          }
          rv=sqlite3_step(s);
        } while (rv == SQLITE_ROW);
        strcpy(rr+rrlen, "\"endrows\",");
        rrlen += 10;
      } else {
        // XXX TODO ok vs error !!
        int rowsAffected = sqlite3_total_changes(mydb) - tc0;
        if (rowsAffected > 0) {
          int insertId = sqlite3_last_insert_rowid(mydb);

          strcpy(rr+rrlen, "\"ch2\",");
          rrlen += 6;

          sprintf(nf, "%d", rowsAffected);
          strcpy(rr+rrlen, nf);
          rrlen += strlen(nf);
          strcpy(rr+rrlen, ",");
          ++rrlen;

          sprintf(nf, "%d", insertId);
          strcpy(rr+rrlen, nf);
          rrlen += strlen(nf);
          strcpy(rr+rrlen, ",");
          ++rrlen;
        } else {
          strcpy(rr+rrlen, "\"ok\",");
          rrlen += 5;
        }
      }
    }

    if (rv != SQLITE_OK && rv != SQLITE_DONE) {
      // XXX TODO REPORT CORRECT ERROR
      strcpy(rr+rrlen, "\"error\",0,1,\"--\",");
      rrlen += 17;
    }

    // FUTURE TODO what to do in case this returns an error
    sqlite3_finalize(s);

    // TODO explain why:
    if (rrlen + 40 + NEXT_ALLOC > arlen) {
      arlen += EXTRA_ALLOC + 40 + NEXT_ALLOC;
      myfj->cleanup2 = rr = realloc(rr, arlen);
    }
  }

  strcpy(rr+rrlen, "\"bogus\"]");

  return rr;
}
