CREATE OR REPLACE FUNCTION MCX_ADHOC_ACC_POST(P_DATE DATE,p_user in varchar2,v_type in varchar2,P_err_msg out varchar2) RETURN NUMBER IS 
/* created by PRAMOD  06-NOV-2023*/

--NOTE : ADD IN IN PROCEDURE FOR P_DATE AND P_EXCH AND CHANGE V_TYPE TO SOMETHING DIFF 

v_cntl_no		control_nos.cntl_no%type;
v_cnt number;  
v_insert_cnt number:=0;
v1_insert_cnt number:=0;
v_dr_cr varchar2(2);
v_ref_no_OLD varchar2(100);
V_REC_COUNT NUMBER:=0; 
V_CTV_VCHR_NO number;
v_cas_cnt  number:=0;
V_CNTRL_NO number:=0;

cursor c1 is 
SELECT Entry_date,exch_code, acc_cd,sub_acc_cd, amount,remarks,reference_number,dr_cr ,comm_code 
FROM common_accounting_entries
WHERE Entry_date =P_DATE-- :entry_date--P
AND reference_number IS NULL--P
AND NVL(auth_flag,'N') ='Y'
and exch_code='MCX'  ---ADDED ON 21-apr-2022--P
--AND reference_number like '%/'||v_type||'/%'--P
ORDER BY reference_number;

BEGIN
 /*       BEGIN
          if v_type ='MCG' then 

            reference_number:='SHC/MISC/'||v_type||'/'||COMM_CODE||'/'||to_char(ENTRY_DATE,'DDMMYYYY')||'/'||:CNTRL.CNTRL_NO;
            
          elsif  v_type ='MCNG' then 
                
            reference_number:='SHC/MISC/'||v_type||'/'||COMM_CODE||'/'||to_char(ENTRY_DATE,'DDMMYYYY')||'/'||:CNTRL.CNTRL_NO;
            
          elsif  v_type ='MTD' then 
            
            reference_number:='SHC/MISC/'||v_type||'/'||COMM_CODE||'/'||to_char(ENTRY_DATE,'DDMMYYYY')||'/'||:CNTRL.CNTRL_NO;--:CNTRL.TDS_CD
                
          elsif  v_type ='MTDC' then 
                    
            reference_number:='SHC/MISC/'||v_type||'/'||COMM_CODE||'/'||to_char(ENTRY_DATE,'DDMMYYYY')||'/'||:CNTRL.CNTRL_NO;

          end if;	
        END;---UPDATION OF REFERENCE NO FOR ADHOC CHAGRGES

*/
                                   
SELECT CNTL_NO+1 into V_CNTRL_NO
FROM CONTROL_NOS
WHERE CNTL_KEY='COMM_MISC';

for rec in c1
loop
	
	v_cas_cnt  :=0;
	
	
select count(*) into v_cas_cnt from drr_cas_temp_vchr  -----ADDED ON 21-apr-2022
where CTV_VCHR_DT=rec.Entry_date
--and CTV_REF_NO=rec.reference_number
and CTV_EXCH_CD=rec.exch_code
and CTV_CLIENT_CD=rec.acc_cd
and CTV_TRANS_AMT=rec.amount
and CTV_SB_LEDGER_CD=rec.sub_acc_cd;
	
	if v_cas_cnt>0 then  ---ADDED ON 21-apr-2022
		null;
			      
	else
		
			V_REC_COUNT :=V_REC_COUNT+1;
			
			IF V_REC_COUNT =1 THEN 
				
				v_ref_no_OLD := rec.reference_number;
				
			END IF;
			
			   IF V_REC_COUNT =1  THEN 
						V_CTV_VCHR_NO :=COLL_CHQ_DETAIL.NEXTVAL;
						ELSE
								IF rec.reference_number = v_ref_no_OLD THEN 
								
								V_CTV_VCHR_NO :=COLL_CHQ_DETAIL.CURRVAL;
								
								ELSE
								V_CTV_VCHR_NO :=COLL_CHQ_DETAIL.NEXTVAL;
								
								END IF;
					  END IF;
			
					begin
											INSERT INTO  DRR_CAS_TEMP_VCHR
										     ( CTV_CLIENT_CD,	     CTV_SB_LEDGER_CD,     CTV_BR_CD,			     CTV_VCHR_NO,
										     CTV_VCHR_TYPE,     CTV_TRANS_AMT,     CTV_TOT_AMT,     CTV_VCHR_STATUS,
										     CTV_VCHR_DT,    CTV_REF_NO,	     CTV_VCHR_ORIGIN,	     CTV_EXCH_CD,
										     CTV_DC,	     ctv_ledger_cd,			     
										     CTV_USR_UPD_ID,	     CTV_USR_UPD_DT,
										     ctv_settel_id,		     CTV_COMP_CD,	     CTV_REMARKS  )
										     VALUES
										     (rec.acc_cd,    rec.sub_acc_cd,     1,     V_CTV_VCHR_NO,
										     'PL',      rec.amount,	     rec.amount,			     'CO',
										   --  rec.Entry_date ,   rec.reference_number,     'FR',			     rec.EXCH_CODE,
                                             rec.Entry_date ,   'SHC/MISC/'||'MCNG'||'/'||TO_CHAR(rec.comm_code)||'/'||to_char(rec.Entry_date,'DDMMYYYY')||'/'||TO_CHAR(V_CNTRL_NO),     'FR',			     rec.EXCH_CODE,
										     rec.dr_cr,	     rec.comm_code,--decode(v_type,'MCG',rec.comm_code, 'MCNG',rec.comm_code ,'MTDC',rec.comm_code, null), --P
										       -- :toolbar.curr_user,		     trunc(sysdate),--P
                                               p_user,		     trunc(sysdate),
										     to_char(rec.Entry_date ,'DD-MON-YY'),	     0,		     rec.remarks   );
										     
										  v_insert_cnt := v_insert_cnt + sql%rowcount ; 
											
											if sql%rowcount=0 then                                                    
                                                P_err_msg:='Error: No rows inserted rows('||rec.sub_acc_cd||') in drr_cas_temp_vchr  for acc_code'||rec.acc_cd||sqlcode||'-'||sqlerrm;
                                                DBMS_OUTPUT.PUT_LINE(P_err_msg);
                                                v_insert_cnt := 0;
                                                rollback;
                                                 return(110);
											end if;
									
							exception
									when others then									
										P_err_msg:='Error: No rows inserted rows('||rec.sub_acc_cd||') in drr_cas_temp_vchr  for acc_code'||rec.acc_cd||sqlcode||'-'||sqlerrm;
										DBMS_OUTPUT.PUT_LINE(P_err_msg);
                                        v_insert_cnt := 0;
										rollback;
									 return(115);
							end;
							
					v_ref_no_OLD :=rec.reference_number;
end if; -----ADDED ON -21-apr-2022					
end loop;
return(100);
v1_insert_cnt:=v_insert_cnt;
DBMS_OUTPUT.PUT_LINE('INSERTED COUNT IN DRR_CAS_TEMP_VCHR : '||P_err_msg);
exception
when others then
P_err_msg:='Error in ADHOC CHARGES Acount Posting..'||sqlcode||'-'||sqlerrm;
v_insert_cnt := 0;
rollback;
return(-99);
END;