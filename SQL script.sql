use NG_SUZDAL_DATA;
go

--this script is debugging of working code for final project
--data were loaded into [df] table

declare @Cb as float = 0.37
declare @Mr as int = 2

--;with sorted_df as (select * from df order by 1,2)

--IF OBJECT_ID('dbo.#df') IS NOT NULL 
DROP TABLE #df
create table #df(hole_id nvarchar(16), depth_from float, depth_to float, sample_id nvarchar(16), result float)

insert into #df
select * from df order by 1,2

--step1
;with df1 as (select 
	*
	,case 
		when [result] >= @Cb then 1 else 0 
	end as step1
from #df
),
df2 as (
select 
	df1.*
	,case
		
		when 
			(df1.depth_from <> (select min(t2.depth_from) from df1 t2 where t2.hole_id = df1.hole_id)) and 
			(df1.step1 = 1) and (
			(
				((df1.depth_to - (select t2.depth_from from df1 t2 where t2.hole_id = df1.hole_id and t2.depth_to = df1.depth_from ))>=@Mr) and ((select t2.result from df1 t2 where t2.hole_id = df1.hole_id and t2.depth_to = df1.depth_from)>=@Cb)
			)
			or
			(
				(((select t2.depth_to from df1 t2 where t2.hole_id = df1.hole_id and t2.depth_from = df1.depth_to ) - df1.depth_from)>=@Mr) and ((select t2.result from df1 t2 where t2.hole_id = df1.hole_id and t2.depth_from = df1.depth_to)>=@Cb)
			)
			)
			then 1
			else 0
	end as step2
	 from df1
	 ),
df3 as (
select 
	df2.*
	,case 
		
		when 
			(df2.depth_from <> (select min(t2.depth_from) from df2 t2 where t2.hole_id = df2.hole_id)) 
			and result < @Cb 
			and (select t2.result from df2 t2 where t2.hole_id = df2.hole_id and t2.depth_from = df2.depth_to)>=@Cb 
			and (select t2.result from df2 t2 where t2.hole_id = df2.hole_id and t2.depth_to = df2.depth_from)>=@Cb
		then 1
		else step2 end step3

from df2)
select 
	df3.*
	,case
		when
			(df3.depth_from <> (select min(t2.depth_from) from df3 t2 where t2.hole_id = df3.hole_id)) 
			and ((df3.step1 = 1) or (df3.step3 = 1))
			and 
			(
				((select t2.step3 from df3 t2 where t2.hole_id = df3.hole_id and t2.depth_to = df3.depth_from)<>0) 
				or ((select t2.step3 from df3 t2 where t2.hole_id = df3.hole_id and t2.depth_from = df3.depth_to)<>0)
				or ( 
					((select t2.step1 from df3 t2 where t2.hole_id = df3.hole_id and t2.depth_to = df3.depth_from)<>0)
					and ((select t2.step1 from df3 t2 where t2.hole_id = df3.hole_id and t2.depth_from = df3.depth_to)<>0)
					and ( (select t2.depth_to from df3 t2 where t2.hole_id = df3.hole_id and t2.depth_from = df3.depth_to)
							-(select t2.depth_from from df3 t2 where t2.hole_id = df3.hole_id and t2.depth_to = df3.depth_from) ) >= @Mr
					)
			) then 1 else 0 end composite
from df3 order by 1,2


--#     if (
--#             (
--#                     (df["step1"][i] == 'Ð?') or (df["step3"][i] == 'Ê')
--#             )
--#             and
--#             (
--#                     (df["step3"][i - 1] != 'Ï')
--#                     or (df["step3"][i + 1] != 'Ï')
--#                     or ((df["step1"][i - 1] == 'Ð?') and (df["step1"][i + 1] == 'Ð?') and (
--#                     (df["To_Depth"][i + 1] - df["From_Depth"][i - 1]) >= Mr))
--#             )
--#     ):
--#         df["composite"][i] = 'Ê'