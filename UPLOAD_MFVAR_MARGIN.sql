create or replace FUNCTION UPLOAD_MFVAR_MARGIN(P_DATE DATE,P_FILE_NAME IN VARCHAR2,P_USER VARCHAR2,P_EXCHANGE_CODE IN VARCHAR2,V_ERROR_MESSAGE OUT VARCHAR2) return PLS_INTEGER AS
/*
VERSION:       1.0
VERSION:       2.0  :- PRAMOD SARGAR FOR NEW FILE FORMAT ON 22SEP2023
DATE OF CREATION : 21-AUG-2013
PURPOSE: LOADING MF SECURITY VAR MARGIN FILE
BY : VIGHNESH
*/

/*
SET SERVEROUTPUT ON
DECLARE
P_DATE DATE:=TRUNC(SYSDATE);
P_FILE_NAME VARCHAR2(100):='MF_VAR_20092023.csv';
P_USER VARCHAR2(5):='PPS';
P_EXCHANGE_CODE  VARCHAR2(5):='NSE';
V_ERROR_MESSAGE  VARCHAR2(1000);
V_RETURN PLS_INTEGER;
BEGIN
V_RETURN:=UPLOAD_MFVAR_MARGIN(P_DATE,P_FILE_NAME,P_USER,P_EXCHANGE_CODE,V_ERROR_MESSAGE);
DBMS_OUTPUT.PUT_LINE('ERROR FILE SUCCESFULLY OPENED'||V_RETURN);
DBMS_OUTPUT.PUT_LINE('ERROR FILE SUCCESFULLY OPENED'||V_ERROR_MESSAGE);
END;
*/

/*
SET SERVEROUTPUT ON
DECLARE
P_DATE DATE:=TRUNC(SYSDATE);
P_FILE_NAME VARCHAR2(100):='MF_VAR_20092023.csv';
P_USER VARCHAR2(5):='PPS';
P_EXCHANGE_CODE  VARCHAR2(5):='NSE';
V_ERROR_MESSAGE  VARCHAR2(1000);
*/
V_ERROR_FILE_HANDLE     UTL_FILE.FILE_TYPE;
V_FILE_HANDLE           UTL_FILE.FILE_TYPE;
V_ERROR_FILE_DIR        VARCHAR2(30) := '/usr/shcil/deriv/'||p_exchange_code||'/err';
V_INPUT_FILE_DIR        VARCHAR2(30) := '/usr/shcil/deriv/'||p_exchange_code||'/in';

TYPE T_TEMP IS TABLE OF VARCHAR2(50) INDEX BY BINARY_INTEGER;
V_TEMP            	 	T_TEMP;
V_CNT               	pls_integer:=0;
v_buffer              	varchar2(1000);
V_LIMIT               	pls_integer;
V_I                 	pls_integer;
V_ERROR_FILE_NAME       VARCHAR2(100);
v_count            		pls_integer;
V_DUMMY_VARIABLE        number;
v_counter       		number:=0;
v_symbol              	mkt_summary.symbol%type;
v_cn         			number:=0;
v_dt        			date;
v_rpt					number:=0;

BEGIN

  V_ERROR_FILE_NAME := p_exchange_code||'_SEC_MASTER_MF.ERR';

  if p_exchange_code = 'MSX' then
		V_ERROR_FILE_DIR := '/usr/shcil/deriv/MCX/err';
		V_INPUT_FILE_DIR := '/usr/shcil/deriv/MCX/in';
  end if;

  BEGIN
   V_ERROR_FILE_HANDLE  := UTL_FILE.FOPEN(V_ERROR_FILE_DIR,V_ERROR_FILE_NAME,'W');
     DBMS_OUTPUT.PUT_LINE('ERROR FILE SUCCESFULLY OPENED');
  EXCEPTION
   WHEN OTHERS THEN
   V_ERROR_MESSAGE := 'ERROR : ERROR IN OPENING ERROR FILE : '||V_ERROR_FILE_NAME;
   DBMS_OUTPUT.PUT_LINE(V_ERROR_MESSAGE);
   return(-116);
  END;

          BEGIN
           V_FILE_HANDLE   := UTL_FILE.FOPEN(V_INPUT_FILE_DIR,P_FILE_NAME,'R');
           DBMS_OUTPUT.PUT_LINE('FILE SUCCESFULLY OPENED');
          EXCEPTION
           WHEN OTHERS THEN
           V_ERROR_MESSAGE := 'ERROR : ERROR IN OPENING FILE '||P_FILE_NAME;
           UTL_FILE.PUT_LINE(V_ERROR_FILE_HANDLE,V_ERROR_MESSAGE);
           DBMS_OUTPUT.PUT_LINE('THE ERROR IS : '||V_ERROR_MESSAGE);
           BEGIN
            UTL_FILE.FFLUSH(V_ERROR_FILE_HANDLE);
            UTL_FILE.FCLOSE(V_ERROR_FILE_HANDLE);
           EXCEPTION
            WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('ERROR : UNABLE TO CLOSE ERROR FILE : '||SQLERRM);
            DBMS_OUTPUT.PUT_LINE('THE ERROR IS : '||V_ERROR_MESSAGE);
            return(-117);
           END;
          END;

  BEGIN

 /*   UTL_FILE.GET_LINE(V_FILE_HANDLE,V_BUFFER);
    V_CNT   := 1;
    V_BUFFER := V_BUFFER || ',';
--    V_COUNTER := V_COUNTER + 1;

-- dbms_output.put_line('v_buffer '||v_buffer);
         FOR I IN 1..5
           LOOP
             SELECT INSTR(V_BUFFER,',',1,I) INTO V_LIMIT FROM DUAL;

             V_TEMP(I) := ltrim(rtrim(SUBSTR(V_BUFFER,V_CNT,V_LIMIT - V_CNT))) ;
                   dbms_output.put_line(V_TEMP(I));
             V_CNT   := V_LIMIT + 1;
           END LOOP;


select FUNCTION_NEXT_DATE(to_date(v_temp(2),'DDMMYYYY'),p_exchange_code) into v_dt from dual;
*/
UTL_FILE.GET_LINE(V_FILE_HANDLE,V_BUFFER);
--select FUNCTION_NEXT_DATE(trunc(sysdate),p_exchange_code) into v_dt from dual;--N
        BEGIN 
            select FUNCTION_NEXT_DATE(P_DATE,p_exchange_code) into v_dt from dual;
            EXCEPTION
            WHEN OTHERS THEN
            dbms_output.put_line('IN DATE SELECTION BLOCK ');
            v_dt:=TRUNC(SYSDATE)+1;
        END;
   LOOP
   V_CNT   := 1;
   UTL_FILE.GET_LINE(V_FILE_HANDLE,V_BUFFER);
    V_BUFFER := V_BUFFER || ',';
    v_counter:=v_counter+1;

        FOR I IN 1..6--10--P
           LOOP
             SELECT INSTR(V_BUFFER,',',1,I) INTO V_LIMIT FROM DUAL;
             V_TEMP(I) := ltrim(rtrim(SUBSTR(V_BUFFER,V_CNT,V_LIMIT - V_CNT))) ;
             V_CNT   := V_LIMIT + 1;
           END LOOP;
   BEGIN


        BEGIN
		      if v_temp(4) != 'EQ' then--if v_temp(3) != 'EQ' then --P
                      v_cn:=v_cn+1;

					  select count(*)
					  into v_rpt
					  from sec_var_margin
					  where load_date = V_dt--trunc(sysdate)--V_dt --P
					  and exchange_code = p_exchange_code
					  and isin = V_TEMP(1);--and isin = V_TEMP(4); --P
					  --***bez data already loaded in EQ not again loaded in MF
					  if v_rpt > 0 then
					    goto next_rec;
					  end if;

                /*   dbms_output.put_line('Before Insert record '||v_cn||'---->'||
        		       V_TEMP(1)||','||V_TEMP(2)||','||V_TEMP(3)||','||V_TEMP(4)||','||V_TEMP(5)||','
					   ||V_TEMP(6)||','||V_TEMP(9)||','||V_TEMP(8)||','||V_TEMP(7));*/ --P
                       
                    dbms_output.put_line('Before Insert record '||v_cn||'---->'||
        		       V_TEMP(1)||','||V_TEMP(2)||','||V_TEMP(3)||','||V_TEMP(4)||','||V_TEMP(5)||','
					   ||V_TEMP(6));  --P

					    insert into sec_var_margin
						(LOAD_DATE,ISIN,SERIES,SEC_VAR,INDEX_VAR,VAR_MARGIN,ADHOC_MARGIN,MARGIN_RATE,
						 LAST_UPD_DATE,LAST_UPD_USER,EXCHANGE_CODE,TYPE,SYMBOL,PRICE)
					     values
					/*	(V_dt,V_TEMP(4),V_TEMP(3),to_number(V_TEMP(5)),to_number(V_TEMP(6)),to_number(V_TEMP(9)),
						 to_number(V_TEMP(8)),to_number(V_TEMP(7)),
                         trunc(sysdate),p_user,p_exchange_code,'MF');  */ --P
                        
                        -- V_dt HAS BEEN REPLACED BY SYSDATE BY --P
                        -- 'MF' HAS BEEN REPLACED BY V_TEMP(4) BY --P
                        (V_dt,V_TEMP(1),V_TEMP(3),to_number(V_TEMP(5)),NULL,NULL,NULL,to_number(V_TEMP(5)),
                        trunc(sysdate),p_user,p_exchange_code,'MF'--V_TEMP(4)--NEW
                        ,V_TEMP(2),V_TEMP(6));   --P
                        
                 IF sql%rowcount = 0 then
                  dbms_output.put_line('***** sql%rowcount = 0');
				  return(0);
                 END IF;
             end if;
          END;


    EXCEPTION WHEN OTHERS THEN
        v_error_message :='Error in Inserting '||sqlerrm;
        dbms_output.put_line(v_error_message);
        BEGIN
               UTL_FILE.FFLUSH(V_ERROR_FILE_HANDLE);
             UTL_FILE.FCLOSE(V_ERROR_FILE_HANDLE);
        EXCEPTION
                WHEN OTHERS THEN
              V_ERROR_MESSAGE := 'ERROR : UNABLE TO CLOSE ERROR FILE : '||SQLERRM;
              DBMS_OUTPUT.PUT_LINE(V_ERROR_MESSAGE);
              ROLLBACK;
              return(-119);
        END;
 END;
 <<next_rec>>
    null;
  END LOOP;
  EXCEPTION 
  WHEN NO_DATA_FOUND THEN
     dbms_output.put_line('End of file'||v_counter);
  UTL_FILE.FCLOSE(V_FILE_HANDLE);
  WHEN OTHERS THEN
     V_ERROR_MESSAGE := 'ERROR : UNABLE TO read file : '||SQLERRM;
  DBMS_OUTPUT.PUT_LINE(V_ERROR_MESSAGE);
  ROLLBACK;
  return(-120);
  END;
  dbms_output.put_line('Dt : '||v_dt);

        BEGIN
           UTL_FILE.FFLUSH(V_ERROR_FILE_HANDLE);
           UTL_FILE.FCLOSE(V_ERROR_FILE_HANDLE);
        EXCEPTION
           WHEN OTHERS THEN
         V_ERROR_MESSAGE := 'ERROR : UNABLE TO CLOSE ERROR FILE : '||SQLERRM;
         DBMS_OUTPUT.PUT_LINE(V_ERROR_MESSAGE);
         ROLLBACK;
         return(-123);
       END;
return(100);
END;