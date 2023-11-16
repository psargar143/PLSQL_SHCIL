SET SERVEROUT ON SIZE 100000;
DECLARE

TYPE CONF_RECORD_TYPE IS RECORD (
                                    TRANS_PTS    number,
                                    COUNT_PTS  number,
                                    QTY_PTS    number,
                                    TRANS_SCS    number,
                                    COUNT_SCS  number,
                                    QTY_SCS    number
                                 );
CONF_RECORD CONF_RECORD_TYPE;

TYPE CONF_COLLECTION_TYPE IS TABLE OF CONF_RECORD_TYPE;
CONF_DATA        CONF_COLLECTION_TYPE;
NET_DATA         CONF_COLLECTION_TYPE;
PMS_NET_DATA     CONF_COLLECTION_TYPE;

TYPE CONF_CURSOR_TYPE IS REF CURSOR;
CONF_CURSOR     CONF_CURSOR_TYPE;
NET_CURSOR      CONF_CURSOR_TYPE;
PMS_NET_CURSOR  CONF_CURSOR_TYPE;

cursor c1 is 
select distinct nse_setl_start,NSE_NUM_SETL_NO--distinct nse_setl_start 
from nse_setl_master_elec
where nse_final_obg >= trunc(sysdate)
and nse_setl_start = trunc(sysdate)
and nse_exch_code in ('NSE','BSE')
AND NSE_SETL_TYPE='G';

BEGIN

FOR I IN C1 
LOOP
dbms_output.put_line('----------------------------------------------------------------------------------------------------------------------------------------');
dbms_output.put_line('CONFIRMATION DATE  :- '||i.nse_setl_start);
dbms_output.put_line('----------------------------------------------------------------------------------------------------------------------------------------');
dbms_output.put_line('SETL ID  :- '||i.NSE_NUM_SETL_NO);
dbms_output.put_line('----------------------------------------------------------------------------------------------------------------------------------------');
dbms_output.put_line(chr(9)||chr(9)||'     ||PTS END||'||chr(9)||chr(9)||chr(9)||chr(9)||chr(9)||chr(9)||chr(9)||'      ||SCS END||'||chr(9)||chr(9)||chr(9)||chr(9)||chr(9)||chr(9)||chr(9));
dbms_output.put_line('-----------------------------------------------------------------------------------------------------------------------------------------');
dbms_output.put_line('TRANSFER ID'||chr(9)||chr(9)||'COUNT'||chr(9)||chr(9)||chr(9)||'QTY'||chr(9)||chr(9)||chr(9)||'TRANSFER ID'||chr(9)||chr(9)||'COUNT'||chr(9)||chr(9)||'          QTY'||chr(9));
dbms_output.put_line('----------------------------------------------------------------------------------------------------------------------------------------');
dbms_output.put_line(chr(9)||chr(9)||chr(9)||chr(9)||chr(9)||chr(9)||chr(9)||'  CONFIRMED ORDERS'||chr(9)||chr(9)||chr(9)||chr(9)||chr(9));
dbms_output.put_line('----------------------------------------------------------------------------------------------------------------------------------------');
OPEN CONF_CURSOR FOR
         WITH pts AS (SELECT NVL(TRANSFER_ID,0) TRANSFER_ID_PTS,COUNT(*) COUNT_PTS,SUM(QTY) QTY_PTS FROM CONFIRMED_ORDERS
            WHERE SETL_NO in (SELECT NSE_SETL_ID FROM nse_setl_master_elec Where NSE_SETL_START=TRUNC(SYSDATE) AND nse_setl_type IN ('G','J','K'))
            GROUP BY TRANSFER_ID ORDER BY 1
            ),
     scs AS (SELECT NVL(TRANSFER_ID,0) TRANSFER_ID_SCS,COUNT(*) COUNT_SCS,SUM(QTY) QTY_SCS FROM CONFIRMED_ORDERS@pts_to_scs
            WHERE SETL_NO in (SELECT NSE_SETL_ID FROM nse_setl_master_elec WHERE NSE_SETL_START=TRUNC(SYSDATE) AND nse_setl_type IN ('G','J','K'))
            GROUP BY TRANSFER_ID ORDER BY 1
                ) 
SELECT nvl(TRANSFER_ID_PTS,0) TRANSFER_ID_PTS, nvl(COUNT_PTS,0) COUNT_PTS, nvl(QTY_PTS,0) QTY_PTS, TRANSFER_ID_SCS, COUNT_SCS, QTY_SCS
FROM pts FULL JOIN scs ON pts.TRANSFER_ID_PTS=scs.TRANSFER_ID_SCS order by 1;

LOOP   
FETCH CONF_CURSOR
bulk collect into CONF_DATA;--CONF_RECORD.TRANS_PTS,CONF_RECORD.COUNT_PTS,CONF_RECORD.QTY_PTS;
EXIT WHEN CONF_CURSOR%NOTFOUND;
END LOOP;
CLOSE CONF_CURSOR;
for i in 1..CONF_DATA.count loop
dbms_output.put_line(CONF_DATA(i).TRANS_PTS||chr(9)||chr(9)||'|  '||chr(9)||CONF_DATA(i).COUNT_PTS||chr(9)||chr(9)||'|  '||chr(9)||LPAD(CONF_DATA(i).QTY_PTS,10)||chr(9)||'||  '||chr(9)||CONF_DATA(i).TRANS_SCS||chr(9)||chr(9)||'|  '||chr(9)||CONF_DATA(i).COUNT_SCS||chr(9)||chr(9)||'|  '||chr(9)||LPAD(CONF_DATA(i).QTY_SCS,10)||chr(9)||'|  ');
END LOOP;
dbms_output.put_line('----------------------------------------------------------------------------------------------------------------------------------------');
dbms_output.put_line(chr(9)||chr(9)||chr(9)||chr(9)||chr(9)||chr(9)||chr(9)||'   NET POSITIONS'||chr(9)||chr(9)||chr(9)||chr(9)||chr(9));
dbms_output.put_line('----------------------------------------------------------------------------------------------------------------------------------------');
OPEN NET_CURSOR FOR
        WITH pts AS (SELECT NVL(TRANSFER_ID,0) TRANSFER_ID_PTS,COUNT(*) COUNT_PTS,SUM(QTY) QTY_PTS FROM NET_POSITIONS
            WHERE SETL_NO in (SELECT NSE_SETL_ID FROM nse_setl_master_elec Where NSE_SETL_START=TRUNC(SYSDATE) AND nse_setl_type IN ('G','J','K'))
            GROUP BY TRANSFER_ID ORDER BY 1
            ),
     scs AS (SELECT NVL(TRANSFER_ID,0) TRANSFER_ID_SCS,COUNT(*) COUNT_SCS,SUM(QTY) QTY_SCS FROM NET_POSITIONS@pts_to_scs
            WHERE SETL_NO in (SELECT NSE_SETL_ID FROM nse_setl_master_elec WHERE NSE_SETL_START=TRUNC(SYSDATE) AND nse_setl_type IN ('G','J','K'))
            GROUP BY TRANSFER_ID ORDER BY 1
                ) 
SELECT *
FROM pts FULL JOIN scs ON pts.TRANSFER_ID_PTS=scs.TRANSFER_ID_SCS;
LOOP   
FETCH NET_CURSOR 
bulk collect into NET_DATA;--CONF_RECORD.TRANS_PTS,CONF_RECORD.COUNT_PTS,CONF_RECORD.QTY_PTS;
EXIT WHEN NET_CURSOR%NOTFOUND;
END LOOP;
CLOSE NET_CURSOR;
for i in 1..NET_DATA.count loop
dbms_output.put_line(NET_DATA(i).TRANS_PTS||chr(9)||chr(9)||'|  '||chr(9)||NET_DATA(i).COUNT_PTS||chr(9)||chr(9)||'|  '||chr(9)||LPAD(NET_DATA(i).QTY_PTS,10)||chr(9)||'||  '||chr(9)||NET_DATA(i).TRANS_SCS||chr(9)||chr(9)||'|  '||chr(9)||NET_DATA(i).COUNT_SCS||chr(9)||chr(9)||'|  '||chr(9)||LPAD(NET_DATA(i).QTY_SCS,10)||chr(9)||'|  ');
END LOOP;
dbms_output.put_line('----------------------------------------------------------------------------------------------------------------------------------------');
dbms_output.put_line(chr(9)||chr(9)||chr(9)||chr(9)||chr(9)||chr(9)||chr(9)||'PMS NET POSITIONS'||chr(9)||chr(9)||chr(9)||chr(9)||chr(9));
dbms_output.put_line('----------------------------------------------------------------------------------------------------------------------------------------');
OPEN PMS_NET_CURSOR FOR
        WITH pts AS (SELECT NVL(PNP_TRANSFER_ID,0) TRANSFER_ID_PTS,COUNT(*) COUNT_PTS,SUM(PNP_QTY) QTY_PTS FROM PMS_NET_POSITIONS
            WHERE PNP_SETL_ID in (SELECT NSE_SETL_ID FROM nse_setl_master_elec Where NSE_SETL_START=TRUNC(SYSDATE) AND nse_setl_type IN ('G','J','K'))
            GROUP BY PNP_TRANSFER_ID ORDER BY 1
            ),
     scs AS (SELECT NVL(PTS_TRANS_ID,0) TRANSFER_ID_SCS,COUNT(*) COUNT_SCS,SUM(QTY) QTY_SCS FROM NSDL_PP_TXN@pts_to_scs
            WHERE 'N'||SETL_NO in (SELECT NSE_SETL_ID FROM nse_setl_master_elec WHERE NSE_SETL_START=TRUNC(SYSDATE) AND nse_setl_type IN ('G','J','K'))
            GROUP BY PTS_TRANS_ID ORDER BY 1
                ) 
SELECT *
FROM pts INNER JOIN scs ON pts.TRANSFER_ID_PTS=scs.TRANSFER_ID_SCS;
LOOP   
FETCH PMS_NET_CURSOR 
bulk collect into PMS_NET_DATA;
EXIT WHEN PMS_NET_CURSOR%NOTFOUND;
END LOOP;
CLOSE PMS_NET_CURSOR;
for i in 1..PMS_NET_DATA.count loop
dbms_output.put_line(PMS_NET_DATA(i).TRANS_PTS||chr(9)||chr(9)||'|  '||chr(9)||PMS_NET_DATA(i).COUNT_PTS||chr(9)||chr(9)||'|  '||chr(9)||lpad(PMS_NET_DATA(i).QTY_PTS,10)||chr(9)||'||  '||chr(9)||PMS_NET_DATA(i).TRANS_SCS||chr(9)||chr(9)||'|  '||chr(9)||PMS_NET_DATA(i).COUNT_SCS||chr(9)||chr(9)||'|  '||chr(9)||lpad(PMS_NET_DATA(i).QTY_SCS,10)||chr(9)||'|  ');
END LOOP;
dbms_output.put_line('----------------------------------------------------------------------------------------------------------------------------------------');
END LOOP;
END;
/
