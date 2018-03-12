truncate hbd_cnct2;
insert into hbd_cnct2
select distinct on(cnct.file_id, cnct.room_type, cnct.characteristic, cnct.base_board, cnct.amount) 
	md5(cnct.file_id || cnct.room_type || cnct.characteristic || cnct.base_board || cnct.amount), cnct.* from hbd_cnct cnct