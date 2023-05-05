create or replace PROCEDURE SP_DWB_TRAN_DAY AS
    -- 统计日期,数据精度，保留3位小数
    statDate varchar2(10);digit NUMBER := 3;
    -- 供热开始日期,供热结束日期,供暖天数,当前年度
    yearBeginDate varchar2(10);yearEndDate varchar2(10);days number;year varchar2(10);
    -- 换热站编码、换热站名称、网口表编码、热网编码、热网名称、分公司编码、分公司名称,换热站所属子公司编码
    code varchar2(50);name varchar2(50);netCode varchar2(50);heatNetCode varchar2(50);netName varchar2(50);comCode varchar2(50);comName varchar2(50);
    -- 运行负荷
    runLoad number;
    -- 耗热量、耗水量、耗电量、耗气量、耗蒸汽量、产蒸汽量、运行时长
    heat number;water number;elec number;gas number;steam number;produceSteam number;dura number;
    -- 年度耗热量、年度耗水量、年度耗电量、年度耗气量、年度耗蒸汽量、年度运行时长
    yearHeat number;yearWater number;yearElec number;yearGas number;yearSteam number;yearDura number;
    -- 热单价、水单价、电单价、气单价、蒸汽单价
    heatPrice number;waterPrice number;elecPrice number;gasPrice number;steamPrice number;
    -- 建筑面积、停热面积、供暖面积
    floorArea number;stopHeatingArea number;normalHeatingArea number;
    -- 热单耗、水单耗、电单耗、气单耗
    heatCost number;waterCost number;elecCost number;
    -- 总成本、单位成本、COP、年度COP
    totalCost number;moneyCost number;cop number;yearCop number;
    -- 二次侧电单耗
    secElecCost number;
    -- 换热站Id,热网id
    tranId number;heatNetId number;
    -- 是否为新能源
    isNewEnergy CHAR(1 BYTE);
    beginTime varchar2(50);endTime varchar2(50);

CURSOR cur IS
SELECT DISTINCT ptd.code_ AS code_,ptd.name_ AS name_,ohn.code_ AS heatNetCode,ohn.name_ AS net_name_,
                td.date_ AS date_,ht.name_ AS typeName,rtd.tran_id_ AS tranId,opet.name_ AS energyCategory,
                ohn.id_ AS heatNetId
FROM ods_report_tran_day rtd
         INNER JOIN dwd_prod_tran_day ptd ON ptd.code_ = rtd.code_ and ptd.date_ = to_date(rtd.date_,'yyyy-MM-dd')
         INNER JOIN ods_herp_report_user_settings ohrus ON ptd.code_= ohrus.code_ AND ohrus.dimension_='tranStation'
         INNER JOIN ods_herp_report_user ohru ON ohrus.id_ = ohru.settings_id_ AND ohru.type_ = 0
         INNER JOIN ods_herp_user ohu ON ohu.id_ = ohru.user_id_
         INNER JOIN ods_herp_org uo ON uo.id_ = ohu.org_id_
         INNER JOIN ods_report r ON rtd.date_=r.date_ and r.is_valid_='1' and r.is_finish_='1' AND (ohu.org_id_ = r.company_id_ OR uo.parent_id_ = r.company_id_)
         LEFT JOIN (
            SELECT MAX(type_id_) AS type_id_,tran_code_
            FROM ODS_HERP_NEW_ENERGY_TYPE
            GROUP BY tran_code_
        ) et  ON ptd.code_ = et.tran_code_
         LEFT JOIN ODS_HERP_TYPE ht ON ht.id_ = et.type_id_
         LEFT JOIN dwd_prod_heat_net_day hnd ON ptd.heat_net_code_ = hnd.code_ and hnd.date_ = to_date(rtd.date_,'yyyy-MM-dd')
         LEFT JOIN ods_herp_heat_net ohn ON hnd.code_ = ohn.code_
         LEFT JOIN ods_report_tran_day td ON rtd.is_modify_=1 AND td.date_ = rtd.date_ AND rtd.code_ = td.code_
         LEFT JOIN ods_prod_energy_type opet ON ptd.energy_category_code_ = opet.code_
WHERE rtd.is_modify_=1 AND rtd.tran_id_ IS NOT NULL;

BEGIN

-- 获取当前年度供热时段
SELECT begin_date_,end_date_,year_,CASE WHEN to_char(sysdate,'yyyy-MM-dd') >= end_date_ THEN FN_GET_DATE_DIFF(begin_date_,end_date_) ELSE FN_GET_DATE_DIFF(begin_date_,to_char(sysdate,'yyyy-MM-dd')) END
INTO yearBeginDate,yearEndDate,year,days
FROM dwd_heat_settings
WHERE is_current_='1';
beginTime := to_char(systimestamp, 'yyyymmdd hh24:mi:ss.ff3');
DBMS_OUTPUT.PUT_LINE('1、开始时间：'||beginTime);

FOR result in cur LOOP



DELETE FROM dwb_tran_day WHERE code_=result.code_ AND TO_CHAR(date_,'yyyy-MM-dd')= result.date_;

statDate := result.date_;
            code := result.code_;
            name := result.name_;
            heatNetCode := result.heatNetCode;
            netName := result.net_name_;
            tranId := result.tranId;
            heatNetId := result.heatNetId;
            
--endTime := to_char(systimestamp, 'yyyymmdd hh24:mi:ss.ff3');
--DBMS_OUTPUT.PUT_LINE('1、开始时间：'||beginTime||',结束时间：'||endTime);
--beginTime := to_char(systimestamp, 'yyyymmdd hh24:mi:ss.ff3');
-- 判断换热站是否为新能源(1:新能源,0;传统换热站)
BEGIN
SELECT CASE WHEN NEW_ENERGY_TYPE_CODE_ IS NULL THEN 0 ELSE 1 END INTO isNewEnergy
FROM (
         SELECT NEW_ENERGY_TYPE_CODE_
         FROM DWS_PROD_TRAN_DAY_PROP
         WHERE code_ = result.code_ AND date_ = result.date_
         ORDER BY end_time_ DESC
     )
WHERE ROWNUM  = 1;
EXCEPTION WHEN no_data_found THEN isNewEnergy:=NULL;
END;

--endTime := to_char(systimestamp, 'yyyymmdd hh24:mi:ss.ff3');
--DBMS_OUTPUT.PUT_LINE('2、开始时间：'||beginTime||',结束时间：'||endTime);
--beginTime := to_char(systimestamp, 'yyyymmdd hh24:mi:ss.ff3');

-- 获取换热站所属分公司
BEGIN
SELECT hc.code_,hc.name_ INTO comCode,comName
FROM  DWD_HERP_COM hc
          LEFT JOIN DWS_CHG_TRAN_DAY ctd ON hc.code_ = ctd.com_code_
WHERE ctd.code_ = result.code_ AND ctd.date_ = result.date_;
EXCEPTION WHEN no_data_found THEN comCode:=NULL;comName:=NULL;
END;

--endTime := to_char(systimestamp, 'yyyymmdd hh24:mi:ss.ff3');
--DBMS_OUTPUT.PUT_LINE('3、开始时间：'||beginTime||',结束时间：'||endTime);
--beginTime := to_char(systimestamp, 'yyyymmdd hh24:mi:ss.ff3');

-- 耗量
BEGIN
     SELECT rtd.dura_ AS dura,rtd.heat_ AS heat,rtd.water_ AS water,rtd.elec_ AS elec,rtd.gas_ AS gas,rtd.steam_ AS steam,rtd.produce_steam_ AS produceSteam
     INTO dura,heat,water,elec,gas,steam,produceSteam
     FROM ods_report_tran_day rtd
              LEFT JOIN dwd_prod_tran_day ptd ON rtd.code_= ptd.code_ and ptd.date_ = to_date(rtd.date_,'yyyy-MM-dd')
     WHERE rtd.date_=statdate AND ptd.code_= code;

EXCEPTION WHEN no_data_found THEN dura:=NULL;heat:=NULL;elec:=NULL;water:=NULL;gas:=NULL;steam:=NULL;produceSteam:=NULL;
END;

--endTime := to_char(systimestamp, 'yyyymmdd hh24:mi:ss.ff3');
--DBMS_OUTPUT.PUT_LINE('4、开始时间：'||beginTime||',结束时间：'||endTime);
--beginTime := to_char(systimestamp, 'yyyymmdd hh24:mi:ss.ff3');

-- 单价
BEGIN
     SELECT water_price_ AS waterPrice,elec_price_ AS elecPrice,gas_price_ AS gasPrice
     INTO waterPrice,elecPrice,gasPrice
     FROM ods_report_tran_price
     WHERE statdate >= begin_date_ AND statdate < nvl(end_date_,99999999) AND tran_id_= tranId;
EXCEPTION WHEN no_data_found THEN waterPrice:=NULL;elecPrice:=NULL;gasPrice:=NULL;
END;

--endTime := to_char(systimestamp, 'yyyymmdd hh24:mi:ss.ff3');
--DBMS_OUTPUT.PUT_LINE('5、开始时间：'||beginTime||',结束时间：'||endTime);
--beginTime := to_char(systimestamp, 'yyyymmdd hh24:mi:ss.ff3');

BEGIN
SELECT nd.heat_price_,nd.steam_price_
INTO heatPrice,steamPrice
FROM dwb_net_day nd
         LEFT JOIN dwd_prod_heat_net_day hnd ON nd.code_ = hnd.code_ and hnd.date_ = nd.date_
         LEFT JOIN dwd_prod_tran_day ptd ON ptd.heat_net_code_ = hnd.code_ and ptd.date_ = nd.date_
WHERE to_char(nd.date_,'YYYY-MM-dd') = statDate AND ptd.code_ = code;
EXCEPTION WHEN no_data_found THEN heatPrice:=NULL;steamPrice:=NULL;
END;

--endTime := to_char(systimestamp, 'yyyymmdd hh24:mi:ss.ff3');
--DBMS_OUTPUT.PUT_LINE('6、开始时间：'||beginTime||',结束时间：'||endTime);
--beginTime := to_char(systimestamp, 'yyyymmdd hh24:mi:ss.ff3');

-- 年度耗量
BEGIN
SELECT SUM(heat_),SUM(water_),SUM(elec_),SUM(gas_),SUM(steam_),SUM(dura_)
INTO yearHeat,yearWater,yearElec,yearGas,yearSteam,yearDura
FROM dwb_tran_day
WHERE to_char(date_,'YYYY-MM-dd') BETWEEN yearbegindate AND statDate AND code_ = code;
EXCEPTION WHEN no_data_found THEN yearHeat:=NULL;yearWater:=NULL;yearElec:=NULL;yearGas:=NULL;yearSteam:=NULL;yearDura:=NULL;
END;

--endTime := to_char(systimestamp, 'yyyymmdd hh24:mi:ss.ff3');
--DBMS_OUTPUT.PUT_LINE('7、开始时间：'||beginTime||',结束时间：'||endTime);
--beginTime := to_char(systimestamp, 'yyyymmdd hh24:mi:ss.ff3');

-- 面积
BEGIN
SELECT floor_area_,normal_heating_area_,stop_heating_area_
INTO floorArea,normalHeatingArea,stopHeatingArea
FROM DWS_CHG_TRAN_DAY
WHERE code_ = code AND date_ = statDate;
EXCEPTION WHEN NO_DATA_FOUND THEN floorArea:=NULL;stopHeatingArea:=NULL;normalHeatingArea:=NULL;
END;

--endTime := to_char(systimestamp, 'yyyymmdd hh24:mi:ss.ff3');
--DBMS_OUTPUT.PUT_LINE('8、开始时间：'||beginTime||',结束时间：'||endTime);
--beginTime := to_char(systimestamp, 'yyyymmdd hh24:mi:ss.ff3');

            runLoad := null;
            yearHeat := nvl(yearHeat,0)+nvl(heat,0);
            yearWater := nvl(yearWater,0)+nvl(water,0);
            yearElec := nvl(yearElec,0)+nvl(elec,0);
            yearGas := nvl(yearGas,0)+nvl(gas,0);
            yearSteam := nvl(yearSteam,0)+nvl(steam,0);

-- cop
            IF result.energyCategory = '燃气锅炉' THEN
                cop := FN_DIVISION(heat*1000*1000*100,gas*8300*4.19);
                yearCop := FN_DIVISION(yearHeat*1000*1000*100,yearGas*8300*4.19);
            ELSIF result.energyCategory = '热泵' AND result.typeName = '燃气空气源热泵' THEN
                cop := FN_DIVISION(heat*1000*1000,gas*8300*4.19);
                yearCop := FN_DIVISION(yearHeat*1000*1000,yearGas*8300*4.19);
            ELSIF result.energyCategory = '热泵' AND result.typeName != '燃气空气源热泵' THEN
                cop := FN_DIVISION(heat*1000,elec*3.6);
                yearCop := FN_DIVISION(yearHeat*1000,yearElec*3.6);
            ELSIF result.energyCategory = '电极锅炉'THEN
                cop := FN_DIVISION(heat*1000*100,elec*3.6);
                yearCop := FN_DIVISION(yearHeat*1000*100,yearElec*3.6);
ELSE
                cop := NULL;
                yearCop := NULL;
END IF;

-- 二次侧电单耗
            secElecCost := FN_DIVISION(yearElec*120,normalHeatingArea*days,digit);

            heatCost := FN_DIVISION(heat*120,normalHeatingArea,digit);
            waterCost := FN_DIVISION(water*1000*120,normalHeatingArea,digit);
            elecCost := FN_DIVISION(elec*120,normalHeatingArea,digit);
            moneyCost := FN_DIVISION((NVL(heat,0)+NVL(water*1000,0)+NVL(elec,0)),normalHeatingArea,digit);
            totalCost := NVL(water*waterPrice,0)+NVL(elec*elecPrice,0)+NVL(heat*heatPrice,0)+NVL(gas*gasPrice,0)+NVL(steam*steamPrice,0);

--endTime := to_char(systimestamp, 'yyyymmdd hh24:mi:ss.ff3');
--DBMS_OUTPUT.PUT_LINE('9、开始时间：'||beginTime||',结束时间：'||endTime);
--beginTime := to_char(systimestamp, 'yyyymmdd hh24:mi:ss.ff3');

INSERT INTO dwb_tran_day(
    CREATE_TIME_,DATE_,YEAR_BEGIN_DATE_,
    CODE_,NAME_,NET_CODE_,NET_NAME_,COM_CODE_,COM_NAME_,RUN_LOAD_,
    HEAT_,WATER_,ELEC_,GAS_,STEAM_,PRODUCE_STEAM_,DURA_,
    YEAR_HEAT_,YEAR_WATER_,YEAR_ELEC_,YEAR_GAS_,YEAR_STEAM_,YEAR_DURA_,
    HEAT_PRICE_,WATER_PRICE_,ELEC_PRICE_,GAS_PRICE_,STEAM_PRICE_,
    FLOOR_AREA_,STOP_HEATING_AREA_,NORMAL_HEATING_AREA_,
    HEAT_COST_,WATER_COST_,ELEC_COST_,
    TOTAL_COST_,MONEY_COST_,COP_,YEAR_COP_,
    SEC_ELEC_COST_,IS_NEW_ENERGY
)
VALUES(
          systimestamp,TO_DATE(statDate,'YYYY-MM-dd'),TO_DATE(yearBeginDate,'YYYY-MM-dd'),
          code,name,heatNetCode,netName,comCode,comName,runLoad,
          heat,water,elec,gas,steam,produceSteam,dura,
          yearHeat,yearWater,yearElec,yearGas,yearSteam,yearDura,
          heatPrice,waterPrice,elecPrice,gasPrice,steamPrice,
          floorArea,stopHeatingArea,normalHeatingArea,
          heatCost,waterCost,elecCost,
          totalCost,moneyCost,cop,yearCop,
          secElecCost,isNewEnergy
      );
begin
                SP_DWS_PROD_TRAN_HEAT_NET_DAY(code,statDate);
                SP_DWS_PROD_TRAN_YEAR(code,year);
end;
UPDATE ods_report_tran_day SET is_modify_=0 WHERE is_modify_ = 1 AND tran_id_ = tranId AND date_ = statDate;

 MERGE INTO ODS_REPORT_NET_DAY nd USING (
            SELECT code_
            FROM dwd_prod_net_day WHERE heat_net_code_= heatNetCode AND TO_CHAR(date_,'yyyy-MM-dd')=statDate
        )t ON (nd.code_ = t.code_)
        WHEN MATCHED THEN UPDATE SET nd.is_modify_ = 1 WHERE date_=statDate AND is_modify_=0;
COMMIT;
END LOOP;
endTime := to_char(systimestamp, 'yyyymmdd hh24:mi:ss.ff3');
DBMS_OUTPUT.PUT_LINE('1、开始时间：'||beginTime||',结束时间：'||endTime);
END SP_DWB_TRAN_DAY;
