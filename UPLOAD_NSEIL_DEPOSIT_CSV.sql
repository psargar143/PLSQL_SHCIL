create or replace FUNCTION UPLOAD_NSEIL_DEPOSIT_CSV (P_FILE_NAME VARCHAR2,P_DATE DATE,P_SEG VARCHAR2,P_USER VARCHAR2,P_ERROR_MESSAGE OUT VARCHAR2)
RETURN NUMBER AS
/* PURPOSE : LOADING OF NSEIL DEPOSIT CSV FILE
   CREATED BY : ANURAG 19-4-22

   CHANGES ON 21-4-22
   ADDED QUERY FOR CLIENT_CODE INSERTION

   ADDED SEGMENT PARAMETER IN THE FUNCTION 06-may-22.

   LAST CHANGES : Added Header Skip Line  10-may-22

*/


/*
SET SERVEROUTPUT ON;
declare
P_FILE_NAME VARCHAR2(100):='nseildeposit_fno_27-jun-2023.csv';
P_DATE DATE:='27-jun-2023';
P_SEG VARCHAR2(10):='EQ';
P_USER VARCHAR2(10):='DRV';
P_ERROR_MESSAGE VARCHAR2(1000);
*/
V_ERROR_FILE_HANDLE   UTL_FILE.FILE_TYPE ;
V_FILE_HANDLE         UTL_FILE.FILE_TYPE;
V_ERROR_FILE_DIR    VARCHAR2(30) := 'DER_NSE_DIR';--'/USR/SHCIL/DERIV/NSE/IN';--'DER_NSE_DIR';
V_INPUT_FILE_DIR    VARCHAR2(30) := 'DER_NSE_DIR';--'/USR/SHCIL/DERIV/NSE/IN';--'DER_NSE_DIR';
TYPE T_TEMP IS TABLE OF VARCHAR2(2000) INDEX BY BINARY_INTEGER;
V_TEMP    T_TEMP;
V_CNT   	 NUMBER;
V_BUFFER   VARCHAR2(20000);
V_LIMIT  NUMBER;
V_ERROR_FILE_NAME VARCHAR2(1000);
V_COUNTER  NUMBER:=0;
V_CN   NUMBER:=0;
V_REC_AUD NUMBER:=0;
P_ERROR_NUMBER VARCHAR2(500);
ERR_LBL VARCHAR2(50);
V_FLAG  VARCHAR2(10);
V_NT NUMBER:=0;
V_REC NUMBER := 0;
CL_CODE NUMBER(7);
err_flag varchar2(1):='N'; ----06-may-2022
v_rej_cnt number:=0;
CURSOR C1 IS
SELECT * from nseil_deposit where load_date<=function_date(P_DATE,'NSE');
BEGIN
--BEGIN
  DBMS_OUTPUT.ENABLE(null);
  V_ERROR_FILE_NAME := 'UPLOAD_NSEIL_DEPOSIT_CSV.ERR';
    BEGIN
		  V_ERROR_FILE_HANDLE  := UTL_FILE.FOPEN(V_ERROR_FILE_DIR,V_ERROR_FILE_NAME,'W');
		  DBMS_OUTPUT.PUT_LINE('ERROR FILE SUCCESFULLY OPENED');
	  EXCEPTION
        WHEN OTHERS THEN
			P_ERROR_MESSAGE := 'ERROR : ERROR IN OPENING ERROR FILE : '||V_ERROR_FILE_NAME||SQLERRM;
			DBMS_OUTPUT.PUT_LINE(P_ERROR_MESSAGE);
			p_error_number:=-107;
			RETURN  (P_ERROR_NUMBER);
	END;
    BEGIN---TO COUNTER DOUBLE ENTRIES OF SINGLE CLIENT BY PRAMOD ON 27JUN2023
            select count(*) into V_NT
            from nseil_deposit
            where load_date<=function_date(P_DATE,'NSE');

            IF V_NT>0 THEN
            for i in c1 loop
            INSERT INTO nseil_deposit_AUDIT VALUES (i.LOAD_DATE,i.CLIENT_CODE,i.TM_EXCH_CODE,i.TM_NAME,i.AMOUNT,i.COLL_TYPE,i.SEG_TYPE,i.LAST_UPD_USER,i.LAST_UPD_DATE,i.FILE_NAME,'D',P_USER,sysdate);
            If SQL%rowcount >0 then
                            V_REC_AUD := V_REC_AUD + 1;

            end if;
            end loop;
            dbms_output.put_line('inserted  count: '||V_REC_AUD);
            BEGIN
            V_REC_AUD:=0;
            DELETE FROM nseil_deposit where load_date<=function_date(P_DATE,'NSE');
            V_REC_AUD := SQL%ROWCOUNT;
            dbms_output.put_line('deleted count: '||V_REC_AUD);
            EXCEPTION WHEN OTHERS THEN
            P_ERROR_MESSAGE:='ERROR while DELETING PREVIOUS DATA- ERR '||SQLERRM;
            dbms_output.put_line(P_ERROR_MESSAGE);
            UTL_FILE.PUT_LINE(V_ERROR_FILE_HANDLE,P_ERROR_MESSAGE);
                         p_error_number:=-108;
                         RETURN  (P_ERROR_NUMBER);
                         ROLLBACK;
            END;
            END IF;
            P_ERROR_MESSAGE :='  Total no. of insert  AND DELETE FOR AUDIT = '||V_REC_AUD;
                UTL_FILE.PUT_LINE(V_ERROR_FILE_HANDLE,p_ERROR_MESSAGE);
             EXCEPTION WHEN OTHERS THEN
                        P_ERROR_MESSAGE:='ERROR while BACKUP in nseil_deposit_AUDIT- ERR '||SQLERRM;
                        dbms_output.put_line(P_ERROR_MESSAGE);
                        UTL_FILE.PUT_LINE(V_ERROR_FILE_HANDLE,P_ERROR_MESSAGE);
                         p_error_number:=-106;
                         RETURN  (P_ERROR_NUMBER);
                         ROLLBACK;
                        -- goto err;
    END;
  BEGIN
		V_FILE_HANDLE   := UTL_FILE.FOPEN(V_INPUT_FILE_DIR,P_FILE_NAME,'R');
		DBMS_OUTPUT.PUT_LINE('FILE SUCCESFULLY OPENED');
      EXCEPTION
        WHEN OTHERS THEN
			P_ERROR_MESSAGE := 'ERROR : ERROR IN OPENING FILE '||P_FILE_NAME;
			UTL_FILE.PUT_LINE(V_ERROR_FILE_HANDLE,P_ERROR_MESSAGE);
			DBMS_OUTPUT.PUT_LINE('THE ERROR IS : '||P_ERROR_MESSAGE);
			BEGIN
				UTL_FILE.FFLUSH(V_ERROR_FILE_HANDLE);
				UTL_FILE.FCLOSE(V_ERROR_FILE_HANDLE);
				   EXCEPTION WHEN OTHERS THEN
						DBMS_OUTPUT.PUT_LINE('ERROR : UNABLE TO CLOSE ERROR FILE : '||SQLERRM);
						DBMS_OUTPUT.PUT_LINE('THE ERROR IS : '||P_ERROR_MESSAGE);
						p_error_number:=-105;
						RETURN  (P_ERROR_NUMBER);
						--goto err;
   			END;
   END;

 begin
   v_nt:=0;
		select count(*)
		 into v_nt
		 from NSEIL_DEPOSIT
		where file_name = p_file_name ;
		if v_nt > 0 then
			 P_ERROR_MESSAGE := 'File already uploaded for '||p_file_name;
			 DBMS_OUTPUT.PUT_LINE(P_ERROR_MESSAGE);
			 p_error_number:=-113;
			 RETURN  (P_ERROR_NUMBER);
	   end if;
 end;
 UTL_FILE.GET_LINE(V_FILE_HANDLE,V_BUFFER);  --added on 10 may 2022 Header Skip By Anurag (HEADER SKIP)
DBMS_OUTPUT.PUT_LINE('Segment>>'||P_SEG);
 BEGIN
LOOP
		V_CNT   := 1;
		V_BUFFER := ' ';
		UTL_FILE.GET_LINE(V_FILE_HANDLE,V_BUFFER);
		V_BUFFER := V_BUFFER || ',';
		V_counter:=v_counter+1;
		FOR I IN 1..6
           LOOP
             SELECT INSTR (V_BUFFER,',',1,I) INTO  V_LIMIT FROM  DUAL;
           V_TEMP(I) := LTRIM(RTRIM(SUBSTR(V_BUFFER,V_CNT,V_LIMIT - V_CNT))) ;
             V_CNT   := V_LIMIT + 1;
           END LOOP;
		-- DBMS_OUTPUT.PUT_LINE('TRADE_ID ='||v_temp(1)||' TRADE_STATUS='||v_temp(2)||' INSTRUMENT_TYPE='||v_temp(3)||' UNIQUE_IDENTIFIER='||v_temp(4)||' ACCOUNT='||v_temp(17)||' Price='||v_temp(15));

		  err_flag:='N'; --06-may-2022

		  BEGIN
		    DBMS_OUTPUT.PUT_LINE('CLIENT>>'||trim(v_temp(3)));
          DBMS_OUTPUT.PUT_LINE('CLIENT>>'||LPAD(trim(v_temp(3)),5,'0'));
		  DBMS_OUTPUT.PUT_LINE('Segment>>'||P_SEG);
			SELECT max(CLIENT_CODE) into CL_CODE FROM CLIENT_MASTER
			WHERE (EXCHANGE_CLIENT_CODE = trim(v_temp(3))
			OR EXCHANGE_CLIENT_CODE = LPAD(trim(v_temp(3)),5,'0')) --MISSING PREFIX 0 IN THE FILE PRGRM CANT FETCH PROPER CLIENTCODE
			AND EXCHANGE_CODE IN ('NSE','BSE','MCX')
			--AND STATUS = 'A'
			and nvl(segment_type,'EQ')= p_seg;
		  EXCEPTION
		  WHEN NO_DATA_FOUND THEN
		  	 P_ERROR_MESSAGE := 'CLIENT NO NOT FOUND/INACTIVE FOR TM CODE '||V_TEMP(3);
			 DBMS_OUTPUT.PUT_LINE(P_ERROR_MESSAGE);
			 --p_error_number:=-201;
			-- DBMS_OUTPUT.PUT_LINE  (P_ERROR_NUMBER);
			-- ROLLBACK;
			err_flag:='Y';
		  WHEN OTHERS THEN
		  P_ERROR_MESSAGE:='Error in other while selecting client code for TM CODE :'||LPAD(trim(v_temp(3)),5,'0')||' '||sqlerrm;
		   DBMS_OUTPUT.PUT_LINE(P_ERROR_MESSAGE);
		    --p_error_number:=-202;
			 --DBMS_OUTPUT.PUT_LINE  (P_ERROR_NUMBER);
			 err_flag:='Y';
		  END;
		  if err_flag='Y' then --06-may-2022
		  v_rej_cnt:=v_rej_cnt + 1;
		  dbms_output.put_line('TM code in ERROR- '||LPAD(trim(v_temp(3)),5,'0')||'   BYPASS'); --06-may-2022
		  else

		  Begin
			INSERT into NSEIL_DEPOSIT
				(Load_date,
				 TM_EXCH_CODE,
				 CLIENT_CODE,
				 TM_NAME    ,
				 AMOUNT,
				 COLL_TYPE,
				 SEG_TYPE,
				 LAST_UPD_DATE,
				 LAST_UPD_USER,
                 FILE_NAME
                 )
				 VALUES(
				      P_Date,			---If any issue related to date then check for This..
					  trim(v_temp(3)),
                      CL_CODE,
                      trim(v_temp(4)),
					  trim(v_temp(5)),
                      trim(v_temp(6)),
					  P_SEG,
					  trunc(sysdate),
					  p_user,
					  p_file_name
					  );

	       If SQL%rowcount >0 then
				V_REC := V_REC + 1;
				end if;
		 EXCEPTION WHEN OTHERS THEN
		     P_ERROR_MESSAGE:='ERROR while insert- ERR '||'TM_NAME : '||v_temp(3)||'COLL_TYPE: '||trim(v_temp(6))||SQLERRM;
			 DBMS_OUTPUT.PUT_LINE(P_ERROR_MESSAGE);
		 	 p_error_number:=-116;
			 RETURN  (P_ERROR_NUMBER);
			 ROLLBACK;
			-- goto err;
			END;

		end if;
  END LOOP;
EXCEPTION
  WHEN NO_DATA_FOUND THEN
	   dbms_output.put_line('End of file '||v_counter);
	   UTL_FILE.FCLOSE(V_FILE_HANDLE);
   WHEN OTHERS THEN
     P_ERROR_MESSAGE := 'ERROR : UNABLE TO read file : '||SQLERRM;
	 DBMS_OUTPUT.PUT_LINE(P_ERROR_MESSAGE );
     ROLLBACK;
	  p_error_number := -117;
	  RETURN  (P_ERROR_NUMBER);
	 -- goto err;
END;
    P_ERROR_MESSAGE := 'FILE : '||P_FILE_NAME||'  Total no. of insert count = '||V_REC||' ,Reject Count:'||v_rej_cnt;
    UTL_FILE.PUT_LINE(V_ERROR_FILE_HANDLE,p_ERROR_MESSAGE);
    dbms_output.put_line (p_ERROR_MESSAGE);
	BEGIN
		UTL_FILE.FCLOSE(V_FILE_HANDLE);
		DBMS_OUTPUT.PUT_LINE('FILE SUCCESFULLY CLOSED');
		EXCEPTION
		WHEN OTHERS THEN
			P_ERROR_NUMBER := 109;
			P_ERROR_MESSAGE := 'ERROR : ERROR IN CLOSING FILE : '||P_FILE_NAME;
			UTL_FILE.PUT_LINE(V_FILE_HANDLE,P_ERROR_MESSAGE);
			BEGIN
				UTL_FILE.FFLUSH(V_ERROR_FILE_HANDLE);
				UTL_FILE.FCLOSE(V_ERROR_FILE_HANDLE);
			EXCEPTION
				WHEN OTHERS THEN
				DBMS_OUTPUT.PUT_LINE('ERROR : UNABLE TO CLOSE ERROR FILE : '||SQLERRM);
				P_ERROR_NUMBER:=110;
				RETURN  (P_ERROR_NUMBER);
			END;
			RETURN  (P_ERROR_NUMBER);
	END;

   if utl_file.is_open(V_ERROR_FILE_HANDLE) then
	utl_file.fflush(V_ERROR_FILE_HANDLE);
	utl_file.fclose(V_ERROR_FILE_HANDLE);
   end if;
p_error_number := 100;
--<<err>>
dbms_output.put_line (p_error_number||'-  '||P_ERROR_MESSAGE);
RETURN(p_error_number);

END;