create or replace procedure proc_slb_outstanding_mail is
v_msg_db_title varchar2(20000);
v_msg_db      varchar2(20000);
mail_to_db    varchar2(5000);
mail_to_db_cc varchar2(5000);
mail_from_db varchar2(5000);
v_sub_db varchar2(1000);
v_mail_footer varchar2(1000);
v_mail_error  varchar2(1000);
v_status      varchar2(1000);
v_cnt number;
v_file_name varchar2(1000);
v_dt varchar2(200);
v_fin_id varchar2(10);
v_fin varchar2(50);
--V_RO_CNT NUMBER;

cursor c2 is select distinct  SIT_FIN_ID from slb_inst_trades where SIT_REV_SETL_DATE >= trunc(sysdate);

V_TODAY_QTY NUMBER:=0;
V_REMAINED_QTY NUMBER:=0;
V_RO_CNT NUMBER:=0;
V_EXM_CNT NUMBER:=0;

CURSOR C1 IS
select SIT_FIN_ID, SIT_SCHEME_NO, SIT_LEG, SIT_CONTRACT_DATE, SIT_SETL_DATE, SIT_SETL_ID, SIT_REV_SETL_DATE, SIT_SCRIP_CODE, SIT_QTY, SIT_ENT_DATE 
from slb_inst_trades
where sit_rev_setl_date>TRUNC(SYSDATE)
and sit_fin_id='TICOL'
--and sit_scrip_code='MMFS02'
AND SIT_LEG='RO'
and sit_contract_date=TRUNC(SYSDATE);--trunc(sysdate);

 begin
--dbms_output.put_line ('0.1');
  select count(*) into v_cnt from slb_inst_trades where SIT_REV_SETL_DATE >= trunc(sysdate)
  and trunc(sysdate) not in (select holiday_date from holiday_master where exchange_code = 'NSE');

--dbms_output.put_line ('0');
  if v_cnt > 0 then
                select COUNT(*) INTO V_RO_CNT
                from slb_inst_trades
                where sit_rev_setl_date>TRUNC(SYSDATE)
                and sit_fin_id='TICOL'
                AND SIT_LEG='RO'
                and sit_contract_date=TRUNC(SYSDATE);
                
                IF V_RO_CNT>0 THEN
                
                  SELECT COUNT(*) INTO V_EXM_CNT FROM exim_control WHERE table_name='SLB_RO_UPDATE_QTY' AND import_date=TRUNC(sysdate);
                  
                IF V_EXM_CNT=0 THEN 
                    BEGIN                        
                        FOR I IN C1
                        LOOP                        
                        V_TODAY_QTY:=I.SIT_QTY;--111822
                        --V_REMAINED_QTY:=I.SIT_QTY;--111822                        
                        FOR J IN (select SIT_FIN_ID, SIT_SCHEME_NO, SIT_LEG, SIT_CONTRACT_DATE, SIT_SETL_DATE, SIT_SETL_ID, SIT_REV_SETL_DATE, SIT_SCRIP_CODE, SIT_QTY, SIT_ENT_DATE 
                                    from slb_inst_trades
                                    where sit_rev_setl_date>TRUNC(SYSDATE)
                                    and sit_fin_id='TICOL'
                                    and sit_scrip_code=I.sit_scrip_code
                                    and sit_contract_date<>TRUNC(SYSDATE) ORDER BY SIT_REV_SETL_DATE,SIT_SCRIP_CODE,SIT_QTY DESC)
                        LOOP 
                        --IF v_remained_qty<=j. SIT_QTY THEN                        
                        V_REMAINED_QTY:=J.SIT_QTY-V_TODAY_QTY;
                        V_TODAY_QTY:=ABS(V_REMAINED_QTY);
                        DBMS_OUTPUT.put_line(V_REMAINED_QTY);
                        DBMS_OUTPUT.put_line(V_TODAY_QTY);                        
                        IF v_remained_qty<=0 THEN                       
                            update slb_inst_trades
                            set sit_qty=0---I.SIT_QTY
                            where sit_rev_setl_date>TRUNC(SYSDATE)
                            and sit_fin_id='TICOL'
                            and sit_scrip_code=I.SIT_SCRIP_CODE
                            and sit_contract_date<>TRUNC(SYSDATE)
                            and sit_qty=J.SIT_QTY;
                        --v_remained_qty:=J.SIT_QTY-I.SIT_QTY;
                        --     IF  v_remained_qty>0
                        DBMS_OUTPUT.put_line('IF  PART ');                        
                        ELSE --v_remained_qty>0 THEN                        
                            update slb_inst_trades
                            set sit_qty=V_TODAY_QTY--J.SIT_QTY-V_TODAY_QTY---I.SIT_QTY
                            where sit_rev_setl_date>TRUNC(SYSDATE)
                            and sit_fin_id='TICOL'
                            and sit_scrip_code=I.SIT_SCRIP_CODE
                            and sit_contract_date<>TRUNC(SYSDATE)
                            and sit_qty=J.SIT_QTY;
                        DBMS_OUTPUT.put_line('ELSE PART');
                        --ELSIF v_remained_qty<0 THEN
                        --NULL;
                        ----ELSE
                        EXIT;
                        END IF;
                        DBMS_OUTPUT.put_line('INNER LOOP');
                        END LOOP;
                        DBMS_OUTPUT.put_line('OUTER LOOP');
                        END LOOP;
                        UPDATE exim_control SET import_date=TRUNC(sysdate) WHERE table_name='SLB_RO_UPDATE_QTY';
                        commit;
                       EXCEPTION
                       WHEN OTHERS THEN
                       rollback;
                       DBMS_OUTPUT.put_line('ERR IN UPDATING RO BLOCK');
                    END;
                 END IF;   
                END IF;
for d in c2 loop
--select distinct  SIT_FIN_ID  into v_fin_id from slb_inst_trades where SIT_REV_SETL_DATE > trunc(sysdate) and sit_fin_id = 'IFCI';
--v_fin_id := 'IFCI';
v_fin_id := d.sit_fin_id;

 -- dbms_output.put_line ('1');
/*
    select PPMD_RECV1_ADD ,PPMD_RECV2_ADD ,PPMD_SENDER_ADD, PPMD_SUBJECT||'  '||v_fin_id||'   '||trunc(sysdate) ,
   'Regards'||chr(10)||chr(13)||'SLB - Market Ops'
   into mail_to_db ,mail_to_db_cc,mail_from_db,v_sub_db , v_mail_footer
   from  PTS_PROCESS_MAIL_DTLS
   where PPMD_PROCESS_KEY  = decode(v_fin_id,'IFCI','SLB_OUTSTANDING','SLB_OUTSTANDING_'||v_fin_id);
*/

select substr(FIM_FIN_NAME,1,50) into v_fin  from fin_inst_master where fim_fin_id = v_fin_id and rownum <2;

select IMD_MAIL_ID,IMD_MAIL_ID_CC,'market.ops@stockholding.com','SLB Outstanding Report'||'  '||v_fin||'   '||trunc(sysdate) ,
'Regards'||chr(10)||chr(13)||'SLB - Market Ops'
into mail_to_db ,mail_to_db_cc,mail_from_db,v_sub_db , v_mail_footer
from inst_mail_details
where imd_fin_id = v_fin_id;


--dbms_output.put_line ('2');

   select decode(substr(to_char(sysdate,'DD'),1,1),'0',substr(to_char(sysdate,'DD'),2,1),to_char(sysdate,'DD'))||to_char(sysdate,'MonYYYY')||'_'||v_fin_id into v_dt from dual;
--dbms_output.put_line ('3');

   select 'outstanding_slb_'||v_dt into v_file_name from dual;
  -- select decode(v_fin_id,'IFCI','dinesh_g@stockholding.com','pts_it@stockholding.com') into mail_to_db from dual;
  -- select decode(v_fin_id,'IFCI','dinesh_g@stockholding.com','pts_it@stockholding.com') into mail_to_db_cc from dual;
--dbms_output.put_line ('4');
If V_Fin_Id = 'UIC' Then
   --dr---Shell('rsh 10.200.0.30 -l pts "sudo -u ias11g /oracle/middleware/frinst1/config/reports/bin/rwclient.sh server=reportsserver_drtitan_inst report=/web/pts/reports/pts/pts_slb_outstanding_mail_uic.rep userid=dum/shcil123@pts_p6_p8 PARAM1='||Trunc(Sysdate)||' PARAM2='||V_Fin_Id||' desformat=pdf destype=ftp desname=ftp://pftp:pftp@10.200.0.20/dip/'||V_File_Name||'.pdf mode=bitmap > shelldd2.txt"');
   shell('ssh 10.100.0.61 -l pts "sudo -u ias12c /ias12c/middleware/bin/rwclient.sh server=reportsserver_mhptitan11_inst report=/web/pts/reports/pts/pts_slb_outstanding_mail_uic.rep userid=dum/shcil123@pts_p6_p8 PARAM1='||trunc(sysdate)||' PARAM2='||v_fin_id||' desformat=pdf destype=ftp desname=ftp://pftp:pftp@10.100.0.175/dip/'||v_file_name||'.pdf mode=bitmap > shelldd2.txt"');
Elsif V_Fin_Id = 'SBIL' Then
   --dr--shell('rsh 10.200.0.30 -l pts "sudo -u ias11g /oracle/middleware/frinst1/config/reports/bin/rwclient.sh server=reportsserver_drtitan_inst report=/web/pts/reports/pts/pts_slb_outstanding_mail_sbil.rep userid=dum/shcil123@pts_p6_p8 PARAM1='||trunc(sysdate)||' PARAM2='||v_fin_id||' desformat=pdf destype=ftp desname=ftp://pftp:pftp@10.200.0.20/dip/'||v_file_name||'.pdf mode=bitmap > shelldd2.txt"');
   shell('ssh 10.100.0.61 -l pts "sudo -u ias12c /ias12c/middleware/bin/rwclient.sh server=reportsserver_mhptitan11_inst report=/web/pts/reports/pts/pts_slb_outstanding_mail_sbil.rep userid=dum/shcil123@pts_p6_p8 PARAM1='||trunc(sysdate)||' PARAM2='||v_fin_id||' desformat=pdf destype=ftp desname=ftp://pftp:pftp@10.100.0.175/dip/'||v_file_name||'.pdf mode=bitmap > shelldd2.txt"');
elsif v_fin_id = 'TICOL' then
   --dr---Shell('rsh 10.200.0.30 -l pts "sudo -u ias11g /oracle/middleware/frinst1/config/reports/bin/rwclient.sh server=reportsserver_drtitan_inst report=/web/pts/reports/pts/pts_slb_outstanding_mail_ticol.rep userid=dum/shcil123@ptsrac PARAM1='||Trunc(Sysdate)||' PARAM2='||V_Fin_Id||' desformat=pdf destype=ftp desname=ftp://pftp:pftp@10.200.0.20/dip/'||V_File_Name||'.pdf mode=bitmap > shelldd2.txt"');
   shell('ssh 10.100.0.61 -l pts "sudo -u ias12c /ias12c/middleware/bin/rwclient.sh server=reportsserver_mhptitan11_inst report=/web/pts/reports/pts/pts_slb_outstanding_mail_ticol.rep userid=dum/shcil123@ptsrac PARAM1='||trunc(sysdate)||' PARAM2='||v_fin_id||' desformat=pdf destype=ftp desname=ftp://pftp:pftp@10.100.0.175/dip/'||v_file_name||'.pdf mode=bitmap > shelldd2.txt"');
 elsif v_fin_id='UTIMF' then
   shell('ssh 10.100.0.61 -l pts "sudo -u ias12c /ias12c/middleware/bin/rwclient.sh server=reportsserver_mhptitan11_inst report=/web/pts/reports/pts/pts_slb_outstanding_mail_utimf.rep userid=dum/shcil123@ptsrac PARAM1='||trunc(sysdate)||' PARAM2='||v_fin_id||' desformat=pdf destype=ftp desname=ftp://pftp:pftp@10.100.0.175/dip/'||v_file_name||'.pdf mode=bitmap > shelldd2.txt"');
else
   --dr---Shell('rsh 10.200.0.30 -l pts "sudo -u ias11g /oracle/middleware/frinst1/config/reports/bin/rwclient.sh server=reportsserver_drtitan_inst report=/web/pts/reports/pts/pts_slb_outstanding_mail.rep userid=dum/shcil123@ptsrac PARAM1='||Trunc(Sysdate)||' PARAM2='||V_Fin_Id||' desformat=pdf destype=ftp desname=ftp://pftp:pftp@10.200.0.20/dip/'||V_File_Name||'.pdf mode=bitmap > shelldd2.txt"');
   shell('ssh 10.100.0.61 -l pts "sudo -u ias12c /ias12c/middleware/bin/rwclient.sh server=reportsserver_mhptitan11_inst report=/web/pts/reports/pts/pts_slb_outstanding_mail.rep userid=dum/shcil123@ptsrac PARAM1='||trunc(sysdate)||' PARAM2='||v_fin_id||' desformat=pdf destype=ftp desname=ftp://pftp:pftp@10.100.0.175/dip/'||v_file_name||'.pdf mode=bitmap > shelldd2.txt"');
--rsh 10.200.0.30 -l pts "sudo -u ias11g /oracle/middleware/frinst1/config/reports/bin/rwclient.sh server=reportsserver_drtitan_inst report=/web/pts/reports/pts/pts_slb_outstanding_mail.rep userid=dum/shcil123@ptsrac PARAM1=18-DEC-2017 PARAM2=IFCI desformat=pdf destype=ftp desname=ftp://pftp:pftp@10.200.0.20/dip/dinesh.pdf mode=bitmap"
end if;
  Mail_To_Db_Cc :=Mail_To_Db_Cc||',market.ops@stockholding.com,pts_it@stockholding.com';
  --Proc_Mail_Send_Attch(Mail_To_Db,Mail_To_Db_Cc,Mail_From_Db,V_Sub_Db,'Hi,'||Chr(10)||Chr(10)||'Pls find attached here with the SLB Outstanding report'||Chr(10)||Chr(10)||V_Mail_Footer,V_File_Name||'.pdf',V_Status);
  if v_fin_id='UTIMF' then
  proc_slb_pdf_csv_combine_rpt;
  else
  proc_mail_send_attch('MARKET.OPS@stockholding.com','PTS_IT@stockholding.com','PTS_IT@stockholding.com',v_sub_db,'Hi,'||CHR(10)||CHR(10)||'Pls find attached here with the SLB Outstanding report'||CHR(10)||CHR(10)||v_mail_footer,v_file_name||'.pdf',v_status);
  end if;
  end loop;
  end if;
  exception -- main exception --
	when others then
    rollback;
	Dbms_Output.Put_Line ('Error in procedure :'||Sqlerrm);
end;