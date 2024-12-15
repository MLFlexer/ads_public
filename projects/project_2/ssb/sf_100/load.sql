CREATE TABLE customer
(
        C_CUSTKEY       UINTEGER,
        C_NAME          TEXT,
        C_ADDRESS       TEXT,
        C_CITY          TEXT,
        C_NATION        TEXT,
        C_REGION        TEXT,
        C_PHONE         TEXT,
        C_MKTSEGMENT    TEXT,
        PRIMARY KEY     (C_CUSTKEY)
);

CREATE TABLE lineorder
(
    LO_ORDERKEY             UINTEGER,
    LO_LINENUMBER           UTINYINT,
    LO_CUSTKEY              UINTEGER,
    LO_PARTKEY              UINTEGER,
    LO_SUPPKEY              UINTEGER,
    LO_ORDERDATE            DATE,
    LO_ORDERPRIORITY        TEXT,
    LO_SHIPPRIORITY         UTINYINT,
    LO_QUANTITY             UTINYINT,
    LO_EXTENDEDPRICE        UINTEGER,
    LO_ORDTOTALPRICE        UINTEGER,
    LO_DISCOUNT             UTINYINT,
    LO_REVENUE              UINTEGER,
    LO_SUPPLYCOST           UINTEGER,
    LO_TAX                  UTINYINT,
    LO_COMMITDATE           DATE,
    LO_SHIPMODE             TEXT,
    PRIMARY KEY             (LO_ORDERKEY, LO_LINENUMBER)
);

CREATE TABLE part
(
        P_PARTKEY       UINTEGER,
        P_NAME          TEXT,
        P_MFGR          TEXT,
        P_CATEGORY      TEXT,
        P_BRAND         TEXT,
        P_COLOR         TEXT,
        P_TYPE          TEXT,
        P_SIZE          UTINYINT,
        P_CONTAINER     TEXT,
        PRIMARY KEY     (P_PARTKEY)
);

CREATE TABLE supplier
(
        S_SUPPKEY       UINTEGER,
        S_NAME          TEXT,
        S_ADDRESS       TEXT,
        S_CITY          TEXT,
        S_NATION        TEXT,
        S_REGION        TEXT,
        S_PHONE         TEXT,
        PRIMARY KEY     (S_SUPPKEY)
);

CREATE TABLE date
(
        D_DATEKEY            DATE,
        D_DATE               TEXT,
        D_DAYOFWEEK          TEXT,
        D_MONTH              TEXT,
        D_YEAR               USMALLINT,
        D_YEARMONTHNUM       UINTEGER,
        D_YEARMONTH          TEXT,
        D_DAYNUMINWEEK       UTINYINT,
        D_DAYNUMINMONTH      UTINYINT,
        D_DAYNUMINYEAR       USMALLINT,
        D_MONTHNUMINYEAR     UTINYINT,
        D_WEEKNUMINYEAR      UTINYINT,
        D_SELLINGSEASON      TEXT,
        D_LASTDAYINWEEKFL    UTINYINT,
        D_LASTDAYINMONTHFL   UTINYINT,
        D_HOLIDAYFL          UTINYINT,
        D_WEEKDAYFL          UTINYINT,
        PRIMARY KEY          (D_DATEKEY)
);

-- The paths below should be to the relevant generated table files:
COPY customer   FROM '/home/mlflexer/repos/ADS/assignments/duckdb/ads2024-ssb-dbgen/customer.tbl';
COPY supplier   FROM '/home/mlflexer/repos/ADS/assignments/duckdb/ads2024-ssb-dbgen/supplier.tbl';
COPY date       FROM '/home/mlflexer/repos/ADS/assignments/duckdb/ads2024-ssb-dbgen/date.tbl';
COPY lineorder  FROM '/home/mlflexer/repos/ADS/assignments/duckdb/ads2024-ssb-dbgen/lineorder.tbl';
COPY part       FROM '/home/mlflexer/repos/ADS/assignments/duckdb/ads2024-ssb-dbgen/part.tbl';
