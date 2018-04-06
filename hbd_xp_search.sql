
drop function if exists hbd_xp_search(date, date, integer[]); 

CREATE FUNCTION hbd_xp_search(in_date date, out_date date , variadic ages integer[]) 
	RETURNS  table (
		cnct_id varchar, file_id varchar, room_type varchar, characteristic varchar, board varchar, 
		sbprice numeric, sbbprice numeric, pbprice numeric, pbbprice numeric, total_price numeric
	) AS $$
<< outerblock >>
DECLARE
    rec record;
	prev_rec record;
	first_row boolean := true;
	rt_in_proc varchar; -- cnct_id - уникальный для типа комнаты отеля. 
	dsk json; -- json c днями недели и их количеством в период даты пребывания в отеле;
	
	-- храним базовые цены за все ночи
	sbp_g_sad numeric := 0;
	sbbp_g_sad numeric := 0;
	pbp_g_sad numeric := 0;
	pbbp_g_sad numeric := 0;
	
	-- храним базовые цены за все ночи
	sbp numeric := 0;
	sbbp numeric := 0;
	pbp numeric := 0;
	pbbp numeric := 0;
	
	-- храним базовые цены за ночь
	sbp_1 numeric := 0;
	sbbp_1 numeric := 0;
	pbp_1 numeric := 0;
	pbbp_1 numeric := 0;
	
	sad_days integer := 0; -- храним количество скидочных дней
	no_sad_days integer := 0; -- храним количество обычных дней
	-- название предъидущей структуры
	sad_name text := '';
	
	general_sad varchar[] := '{}';
	

BEGIN
	perform _xp_hbd_sad_list( in_date, out_date, 1, 5, 30, 30 );
	--RAISE EXCEPTION 'Нет рейсов на дату: %.', $1;
	for rec in  select * from _hbd_sad_list -- получаем отсортированный список скидок и считаем в цикле
	loop
	raise notice '%', rec;
		-- проверяем, не первая ли запись
		if first_row is true then
			prev_rec := rec;			
		end if;
		-- если пришел новый room_type cnct_id изменяется, выводим суммы
		if  first_row is false and rec.cnct_id != prev_rec.cnct_id then			
			cnct_id := rec.cnct_id; 
			file_id := rec.file_id; 
			room_type := rec.room_type; 
			characteristic := rec.characteristic; 
			board := rec.base_board;
			sbprice := sbp; 
			sbbprice := sbbp; 
			pbprice := pbp;
			pbbprice := pbbp; 
			total_price := sbp + sbbp + pbp + pbbp;
			return next;
		end if;		
		sad_days := 0;
		-- определяем количество дней, когда скидки действуют и не действуют
		for dsk in select * from json_array_elements( rec.bk_days )		
		loop
 			-- если есть скидка за ночь, то выходим из цикла. Валидация была сделеано в запросе
			if rec.application_type = any(array['T', 'U']) then
				sad_days := 1;				
				exit;
			
			-- смотрим, есть ли дни недели в заказе (номера от 0 до 6 с ВС по СБ) среди разрешенных дней скидки 
			elseif (dsk->>'bk_day')::integer = any(rec.week_days) then
				
				sad_days := sad_days + (dsk->>'count_bk_days')::integer;
			end if;			
		end loop;
		
		
		sad_name := rec.sad_name;
		-- считаем базовые цены на номер и завтрак 
		if rec.sad_name = 'cnsr' then
				
				-- цена всех дней для сервиса или человека
				sbp := rec.service_base_price * rec.rest_nights ;	
				sbbp := rec.service_base_board_price * rec.rest_nights;
				pbp := rec.pax_base_price * rec.rest_nights + pbp;
				pbbp := rec.pax_base_board_price * rec.rest_nights + pbbp;
				
				--цена одного дня для сервиса или человека
				sbp_1 := rec.service_base_price;	
				sbbp_1 := rec.service_base_board_price;
				pbp_1 := rec.pax_base_price + pbp_1;
				pbbp_1 := rec.pax_base_board_price + pbbp_1;
				
				--continue;
		elseif rec.sad_name = 'cnsu' then
				--general_sad := '{}';
				--	определяем количество дней со скидками и без скидок sad_days no_sad_days
				
				raise notice '%', 'sad_days ' || sad_days::text;
				if rec.type = any(array['N', 'C', 'I', 'F']) and ( sad_days is not null or sad_days != 0 ) then
				raise notice '%', rec.type;
					-- если скидка на базовую цену
					if rec.application_type = any( array['B', 'N', 'T', 'U'] ) and rec.cnct_is_per_pax = 'Y' and rec.amount is not null then
						pbp := pbp - pbp_1 / rec.paxes * sad_days  + ( pbp_1 / rec.paxes + rec.amount / rec.paxes ) * sad_days;
						raise notice '%', 1;
						
					elseif rec.application_type = any( array['B', 'N', 'T', 'U'] ) and rec.cnct_is_per_pax = 'N' and rec.amount is not null then
						sbp := sbp - sbp_1 / rec.standard_capacity * sad_days + ( sbp_1 / rec.standard_capacity + rec.amount / rec.paxes ) * sad_days;
						raise notice '%', 2;
					elseif rec.application_type = any( array['B', 'N', 'T', 'U'] ) and rec.cnct_is_per_pax = 'Y' and rec.percentage is not null then
						pbp := pbp - pbp_1 / rec.paxes * sad_days + ( pbp_1 / rec.paxes + rec.percentage / 100 * pbp_1 / rec.paxes ) * sad_days;
						raise notice '%', 3;
					elseif rec.application_type = any( array['B', 'N', 'T', 'U'] ) and rec.cnct_is_per_pax = 'N' and rec.percentage is not null then
						sbp := sbp - sbp_1 / rec.standard_capacity * sad_days  + ( sbp_1 / rec.standard_capacity + rec.percentage / 100 * sbp_1 / rec.standard_capacity ) * sad_days;
						raise notice '%', 4 ;--::text || sbp::text;
					end if;
					
					-- если скидка на базовую цену на завтрак
					if rec.application_type = any( array['R', 'N', 'T', 'U'] ) and rec.cnsr_is_per_pax = 'Y' and rec.amount is not null then
						pbbp := pbbp - pbbp_1 / rec.paxes * sad_days  + ( pbbp_1 / rec.paxes + rec.amount / rec.paxes ) * sad_days;
						raise notice '%', 5;
					elseif rec.application_type = any( array['R', 'N', 'T', 'U'] ) and rec.cnsr_is_per_pax = 'N' and rec.amount is not null then
						sbbp := sbbp - sbbp_1 / rec.standard_capacity * sad_days + ( sbbp_1 / rec.standard_capacity + rec.amount / rec.paxes ) * sad_days;
						raise notice '%', 6;
					elseif rec.application_type = any( array['R', 'N', 'T', 'U'] ) and rec.cnsr_is_per_pax = 'Y' and rec.percentage is not null then
						pbbp := pbbp - pbbp_1 / rec.paxes * sad_days + ( pbbp_1 / rec.paxes + rec.percentage / 100 * pbbp_1 / rec.paxes ) * sad_days;
						raise notice '%', 7;
					elseif rec.application_type = any( array['R', 'N', 'T', 'U'] ) and rec.cnsr_is_per_pax = 'N' and rec.percentage is not null then
						sbbp := sbbp - sbbp_1 / rec.standard_capacity * sad_days  + ( sbbp_1 / rec.standard_capacity + rec.percentage / 100 * sbbp_1 / rec.standard_capacity ) * sad_days ;
						raise notice '%', 8;
					end if;
					
					-- абсолютная скидка на базовую цену. Завтрак остается без изменений???
					if rec.application_type = 'A' and rec.cnct_is_per_pax = 'Y' and rec.amount is not null then
						pbp := pbp - pbp_1 / rec.paxes * sad_days + rec.amount / rec.paxes * sad_days;
						--raise notice '%', pbp;
					elseif rec.application_type = 'A' and rec.cnct_is_per_pax = 'N' and rec.amount is not null then
						sbp := sbp - sbp_1 / rec.standard_capacity * sad_days + rec.amount / rec.paxes * sad_days;					
					end if;
					
					-- aбсолютная скидка на базовую цену, но к ней над прибавить базовую цену на завтрак. Сам завтрак остается без изменений???
					if rec.application_type = 'M' and rec.cnct_is_per_pax = 'Y' and rec.cnsr_is_per_pax = 'N'
						and rec.amount is not null then
							pbp := pbp - pbp_1 / rec.paxes * sad_days + ( rec.amount / rec.paxes + pbbp_1 / rec.paxes ) * sad_days;
								raise notice '%', pbp;
					elseif rec.application_type = 'M' and rec.cnct_is_per_pax = 'Y' and rec.cnsr_is_per_pax = 'Y'
						and rec.amount is not null 	then
							pbp := pbp - pbp_1 / rec.paxes * sad_days + ( rec.amount / rec.paxes + sbbp_1 ) * sad_days;
					elseif rec.application_type = 'M' and rec.cnct_is_per_pax = 'N' and rec.cnsr_is_per_pax = 'N'
						and rec.amount is not null then
							sbp := sbp - pbp_1 / rec.standard_capacity * sad_days + 
								( rec.amount / rec.paxes + sbbp_1 / rec.standard_capacity ) * sad_days ;
					elseif rec.application_type = 'M' and rec.cnct_is_per_pax = 'N' and rec.cnsr_is_per_pax = 'Y'
						and rec.amount is not null 	then
							sbp := sbp - pbp_1 / rec.standard_capacity * sad_days + 
								( rec.amount / rec.paxes + pbbp_1 ) * sad_days ;
					end if;
					
				elseif rec.type = any( array[ 'B', 'K', 'U', 'L', 'M', 'O', 'E', 'V', 'G' ] ) and ( sad_days is not null or sad_days != 0 ) then
					
					/*if prev_rec.sad_name != rec.sad_name then 
						general_sad = '{}';
					end if;
					general_sad := array_append(general_sad, rec.type) ;
					*/
					-- если скидка на базовую цену
					if rec.application_type = any( array['B', 'N', 'T', 'U'] ) and rec.cnct_is_per_pax = 'Y' and rec.amount is not null then
						pbp_g_sad := rec.amount * sad_days;
						pbp := pbp + pbp_g_sad;						
						raise notice '%', 11;						
					elseif rec.application_type = any( array['B', 'N', 'T', 'U'] ) and rec.cnct_is_per_pax = 'N' and rec.amount is not null then
						sbp_g_sad := rec.amount * sad_days;
						sbp := sbp + sbp_g_sad;
						raise notice '%', 12;
					elseif rec.application_type = any( array['B', 'N', 'T', 'U'] ) and rec.cnct_is_per_pax = 'Y' and rec.percentage is not null then
						pbp_g_sad := pbp_g_sad * rec.percentage / 100;
						pbp := pbp + pbp_g_sad;
						raise notice '%', 13;
					elseif rec.application_type = any( array['B', 'N', 'T', 'U'] ) and rec.cnct_is_per_pax = 'N' and rec.percentage is not null then
						sbp_g_sad := sbp_g_sad * rec.percentage / 100;
						sbp := sbp + sbp_g_sad;
						raise notice '%', 14 ;--::text || sbp::text;
					end if;
					
					-- если скидка на базовую цену на завтрак
					if rec.application_type = any( array['R', 'N', 'T', 'U'] ) and rec.cnsr_is_per_pax = 'Y' and rec.amount is not null then
						pbbp_g_sad := rec.amount * sad_days;
						pbbp := pbp + pbbp_g_sad;
						raise notice '%', 15;
					elseif rec.application_type = any( array['R', 'N', 'T', 'U'] ) and rec.cnsr_is_per_pax = 'N' and rec.amount is not null then
						sbbp_g_sad := rec.amount * sad_days;
						sbbp := sbbp + sbbp_g_sad;
						raise notice '%', 16;
					elseif rec.application_type = any( array['R', 'N', 'T', 'U'] ) and rec.cnsr_is_per_pax = 'Y' and rec.percentage is not null then
						pbbp_g_sad := pbbp_g_sad * rec.percentage / 100;
						pbbp := pbbp + pbbp_g_sad;
						raise notice '%', 17;
					elseif rec.application_type = any( array['R', 'N', 'T', 'U'] ) and rec.cnsr_is_per_pax = 'N' and rec.percentage is not null then
						sbbp_g_sad := sbbp_g_sad * rec.percentage / 100;
						sbbp := sbbp + sbbp_g_sad;
						raise notice '%', 18;
					end if;
					
					-- абсолютная скидка на базовую цену. Завтрак остается без изменений???
					if rec.application_type = 'A' and rec.cnct_is_per_pax = 'Y' and rec.amount is not null then						
						pbp := rec.amount * sad_days;
						--raise notice '%', pbp;
					elseif rec.application_type = 'A' and rec.cnct_is_per_pax = 'N' and rec.amount is not null then
						sbp := rec.amount * sad_days;					
					end if;
					
					-- aбсолютная скидка на базовую цену, но к ней над прибавить базовую цену на завтрак. Сам завтрак остается без изменений???
					if rec.application_type = 'M' and rec.cnct_is_per_pax = 'Y' and rec.cnsr_is_per_pax = 'N'
						and rec.amount is not null then
							pbp := ( rec.amount + pbbp / rec.paxes ) * sad_days;
							raise notice '%', pbp;
					elseif rec.application_type = 'M' and rec.cnct_is_per_pax = 'Y' and rec.cnsr_is_per_pax = 'Y'
						and rec.amount is not null 	then
							pbp := ( rec.amount + pbbp ) * sad_days;
					elseif rec.application_type = 'M' and rec.cnct_is_per_pax = 'N' and rec.cnsr_is_per_pax = 'N'
						and rec.amount is not null then
							sbp := ( rec.amount + sbbp / rec.standard_capacity ) * sad_days;
					elseif rec.application_type = 'M' and rec.cnct_is_per_pax = 'N' and rec.cnsr_is_per_pax = 'Y'
						and rec.amount is not null 	then
							sbp := ( rec.amount + sbbp ) * sad_days;
					end if;
				end if;
				
		end if;
		first_row := false;
		prev_rec := rec;
	end loop;
    return;
END;
$$ LANGUAGE plpgsql;

select * from hbd_xp_search('2018-08-20'::date, '2018-08-30'::date, 30,30);

--SELECT * FROM tmp_table;
	

	






