--交易流水抽取标准逻辑
CREATE or REPLACE PROCEDURE PROC_LOAD_TRADEDATA_IMSP(I_SSQJ IN VARCHAR2) AS
    V_DCLID NUMBER(20) ;
    V_COUNT INT ;
    V_SSQJ DATE;
    --错误信息
    V_ERROR VARCHAR2(1000);
    V_RDID NUMBER(20);
    V_HSJE NUMBER(20,2);
    V_OIDID NUMBER(20);
    cursor autoRelease is
        select nt.rdid, nr.oidid, nos.hsje
        from dc_tradedata_imsp dt,
             nvat_tradedata_imsp nt,
             nvat_revmapinvoitems nr,
             nvat_outinvoicedetails nod,
             NVAT_OUTINVOICEITEMS nos
        where dt.yjyls = nt.jyls1
         and nt.rdid = nr.rdid
         and nr.oidid = nod.oidid
         and nr.IVIID = nos.IVIID
         AND nr.OIDID =nos.OIDID
         and (nod.fpdm is null or nod.fphm is null)
         and dt.jyrmbje < 0
         and dt.recv_time >= V_SSQJ
         and dt.recv_time < V_SSQJ + 1;
BEGIN
        PROC_LOG('PROC_LOAD_TRADEDATA_IMSP','INFO','0000','1.不动产IMSP交易流水数据抽取逻辑开始,采集参数[' ||I_SSQJ || ']');

        --数据采集日期初始化
        V_SSQJ := trunc(SYSDATE);
        IF LENGTH(I_SSQJ) =8 THEN
            V_SSQJ := to_date(I_SSQJ,'YYYYMMDD');
        END IF;

        PROC_GET_TABLE_SEQ('DC_DATACOLLECTLOG',1,V_DCLID);

        --指定日期是否有待采集数据
        V_COUNT := -1;
        SELECT COUNT(1)
            INTO V_COUNT
            FROM DC_TRADEDATA_IMSP DT
            WHERE NOT EXISTS(SELECT 1 FROM NVAT_TRADEDATA_IMSP NT WHERE DT.JYLS = nt.JYLS1)
            AND RECV_TIME >=V_SSQJ
            AND RECV_TIME < V_SSQJ +1;
        IF V_COUNT > 0 THEN

        --插入个性表
        INSERT INTO NVAT_TRADEDATA_IMSP
            (RDID,--1、主键ID,值与主表一致
             JYLS1, --2、交易流水
             YJYLS,--3、原交易流水
             CPBM,--产品编码
             CPMC,--11、产品名称
             BZ,--备注
             DCID--数据采集号
            )
          SELECT NVAT_TRANDTADVALTAXSEP_S.NEXTVAL AS RDID,
                 A.JYLS AS JYLS1,
                 A.YJYLS AS YJYLS,
                 A.SPBM AS CPBM,
                 A.PRONAME AS CPMC,
                 A.BZ AS BZ,
                 V_DCLID AS DCID
            FROM DC_TRADEDATA_IMSP A,NVAT_PRODUCTCODEINFO C
          WHERE A.SPBM = C.HBCODE
            AND A.RECV_TIME >= V_SSQJ
            AND RECV_TIME < V_SSQJ +1
            AND NOT EXISTS(SELECT 1 FROM  NVAT_TRADEDATA_IMSP WHERE A.JYLS =JYLS1);

        PROC_LOG('PROC_LOAD_TRADEDATA_IMSP','INFO','0000','2.不动产IMSP交易流水数据抽取逻辑个性表数据采集完成,采集ID['||V_DCLID||']');

        --插入共性表
        INSERT  INTO NVAT_TRANDTADVALTAXSEP(
        RDID,
        YWLXBM,
        JYLS,
        KFHM,
        JYRQ,
        JYJG,
        LZJG,
        JYJE,
        BZBM,
        WBHL,
        JYRMBJE,
        SYSL,
        BHSJE,
        XXSE,
        YHCJE,
        WKPJE,
        JDFX,
        KMBM,
        KMMC,
        LSXZ,
        SFCZ,
        CFHBZT,
        LSZT,
        SFSDP,
        YSJYLS,
        JYZH,
        JBRYBM,
        JBBMMC,
        JSFLJG,
        YCYY,
        JSFF,
        ZSXM,
        JZJT,
        ZCBKDK,
        SFYC,
        YZLLX,
        CZGDZC,
        KPYQ,
        JHKPRQ,
        SRKMJDFX,
        SRKM,
        SRKMMC,
        SEKMJDFX,
        SEKM,
        SEKMMC,
        SFCX,
        YTBS1,
        YTBS2,
        YTBS3,
        YTBS4,
        YTBS5,
        YTBS6,
        YTBS7,
        YTBS8,
        YTBS9,
        MKBS,
        SJLY,
        MBBH,
        SPBM,
        GSRQ,
        CREATED_DATE,
        UPDATED_DATE
        )
        SELECT
        B.RDID AS RDID ,
        'BDCIMSP_YWLX_001' AS YWLXBM,
        A.JYLS AS JYLS,
        A.KFHM AS KFHM,
        TRUNC(A.JYRQ) AS JYRQ,
        A.JYJG AS JYJG,
        '' AS LZJG,
        A.JYRMBJE AS JYJE,
        '' AS BZBM,
        1 AS WBHL,
        A.JYRMBJE AS SYSL,
        ROUND(A.JYRMBJE/(1+A.SYSL),2) AS BHSJE,
        A.JYRMBJE - ROUND(A.JYRMBJE/(1+A.SYSL),2) AS XXSE,
        0 AS YHCJE,
        A.JYRMBJE AS WKPJE,
        '' AS JDFX,
        '' AS KMBM,
        '' AS KMMC,
        CASE WHEN A.JYRMBJE>=0 THEN 1 ELSE DECODE(TO_CHAR(A.JYRQ,'YYYYMM'),TO_CHAR(SYSDATE,'YYYYMM'),5,2) END AS LSXZ,
        CASE WHEN A.JYRMBJE>=0 THEN -1 ELSE 0 END AS SFCZ,
        1 AS CFHBZT,
        1 AS LSZT,
        0 AS SFSDP,
        A.YJYLS AS YSJYLS,
        '' AS JYZH,
        '' AS JBRYBM,
        '' AS JBBMMC,
        1 AS JSFLJG,
        '' AS YCYY,
        '' AS JSFF,
        '' AS ZSXM,
        '' AS JZJT,
        '' AS ZCBKDK,
        '' AS SFYZ,
        '' AS YZLLX,
        '' AS CZGDZC,
        A.FPZL AS KPYQ,
        '' AS JHKPRQ,
        '' AS SRKMJDFX,
        '' AS JHKPRQ,
        '' AS SRKM,
        '' AS SRKMMC,
        '' AS SEKMJDFX,
        '' AS SEKM,
        '' AS SEKMMC,
        0 AS SFCX,
        'Y' AS YTBS1,
        'Y' AS YTBS2,
        'N' AS YTBS3,
        'N' AS YTBS4,
        'N' AS YTBS5,
        'N' AS YTBS6,
        'N' AS YTBS7,
        'N' AS YTBS8,
        'N' AS YTBS9,
        1 AS MKBS,
        'BDCIMSP' AS SJLY,
        'BDCIMSP_MB_001' AS MBBH,
        A.SPBM AS SPBM,
        SYSDATE,
        SYSDATE,
        SYSDATE
        FROM DC_TRADEDATA_IMSP A,NVAT_TRADEDATA_IMSP B
        WHERE A.JYLS = B.JYLS1
        AND  A.RECV_TIME >=V_SSQJ AND A.RECV_TIME < V_SSQJ +1
        AND B.DCID =V_DCLID;
    COMMIT ;

    PROC_LOG('PROC_LOAD_TRADEDATA_IMSP','INFO','0000','3.不动产IMSP交易流水数据抽取逻辑共性表数据采集完成,采集ID['||V_DCLID||']');

    OPEN autoRelease;
    LOOP
        FETCH autoRelease INTO V_RDID,V_OIDID,V_HSJE;
        EXIT WHEN autoRelease%NOTFOUND;
        UPDATE NVAT_TRANDTADVALTAXSEP
            SET LSZT =(CASE WHEN(JYRMBJE - V_HSJE) = 0 THEN 1 ELSE 2 END),
                WKPJE = NVL(WKPJE,0) + V_HSJE
        WHERE RDID = V_RDID;
        DELETE FROM NVAT_REVMAPINVOITEMS WHERE OIDID = V_OIDID;
        DELETE FROM NVAT_OUTINVOICEITEMS WHERE OIDID = V_OIDID;
        DELETE FROM NVAT_OUTINVOICEDETAILS WHERE OIDID = V_OIDID;
    END LOOP;
    CLOSE autoRelease;

    PROC_LOG('PROC_LOAD_TRADEDATA_IMSP','INFO','0000','4.不动产IMSP交易负流水,对应源流水待开数据撤销完成');
    COMMIT;

        FOR REC IN ( SELECT
            B.RDID AS RDID,
            B.JYLS AS JYLS,
            B.JYJG AS JYJG,
            B.JYRQ AS JYRQ,
            B.JYRMBJE AS JYRMBJE,
            B.SYSL AS SYSL,
            B.YSJYLS AS YSJYLS
            FROM
                DC_TRADEDATA_IMSP A,NVAT_TRANDTADVALTAXSEP B
            WHERE
                A.JYLS = B.JYLS
                AND B.LSXZ IN (2,5)
                AND B.JYRMBJE < 0
                AND A.RECV_TIME >= V_SSQJ AND A.RECV_TIME < V_SSQJ+1
                )
            LOOP
                V_ERROR :='';

                BEGIN
                    IF REC.YSJYLS IS NOT NULL THEN
                        UPDATE NVAT_TRANDTADVALTAXSEP A SET A.YJYRQ = (SELECT JYRQ FROM NVAT_TRANDTADVALTAXSEP WHERE JYLS = REC.YSJYLS AND ROWNUM =1)
                        WHERE A.RDID = REC.RDID AND JYRQ = REC.JYRQ;
                        COMMIT;
                        PROC_TRADEDATA_NEG_HANDLE(REC.RDID,NULL,REC.JYJG,V_ERROR);
                        IF V_ERROR IS NOT NULL THEN
                            PROC_LOG('PROC_LOAD_TRADEDATA_IMSP','ERROR','9999','4.不动产IMSP红冲流水数据处理失败,NVAT_TRANDTADVALTAXSEP主键RDID[' ||REC.RDID ||']');
                        END IF;
                EXCEPTION WHEN OTHERS THEN
                     PROC_LOG('PROC_LOAD_TRADEDATA_IMSP','ERROR','9999','5.不动产IMSP红冲流水数据处理异常,NVAT_TRANDTADVALTAXSEP主键RDID[' ||REC.RDID ||']' || ',V_ERROR =' || SQLCODE);
                END;
            END LOOP;

            END IF;
                PROC_INSERT_DC_LOG(V_DCLID,'NVAT_TRADEDATA_IMSP',V_COUNT,'存储过程执行成功[PROC_LOAD_TRADEDATA_IMSP]','');
                    PROC_LOG('PROC_LOAD_TRADEDATA_IMSP','INFO','0000','6.不动产IMSP交易流水数据抽取逻辑结束');

                COMMIT;
EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;
    PROC_INSERT_DC_LOG(V_DCLID,'NVAT_TRADEDATA_IMSP',V_COUNT,'存储过程执行失败[PROC_LOAD_TRADEDATA_IMSP]'||SQLCODE||','||SQLERRM,'');
        PROC_LOG('PROC_LOAD_TRADEDATA_IMSP','ERROR','9999','7.不动产IMSP交易流水数据抽取执行异常,SQLCODE['||SQLCODE || ',' || SQLERRM || ']');
        COMMIT;
END PROC_LOAD_TRADEDATA_IMSP;
/


