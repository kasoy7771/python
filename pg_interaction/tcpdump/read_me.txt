Для анализа tcpdump необходимо:
1. Выбрать какую-нибудь развернутую СУБД, например srvdbtest01, или разернуть свою, как больше нравится.
2. зайти в psql в эту базу, сделать там следующие комнады:
create role admin with password '123Qwer' superuser login;
CREATE DATABASE tcpdump;
GRANT ALL PRIVILEGES ON DATABASE tcpdump to admin;
3. Прописать имя юзера, имя бд, имя бд_сервера в config.py
4. Готовим файлы снятые tcpdump
for i in /mnt/tcpdump/*; do echo $i; tcpdump -S -tt -r $i > $i.1c.txt; done
for i in /mnt/tcpdump/*; do echo $i; tcpdump -S -tt -r $i > $i.db.txt; done
5. Загрузить файлы в базу. для этого надо исползовать скрипт load_tcpdupm.py
	Для дб:
python ~/git/python/pg_interaction/tcpdump/load_tcpdump.py -n 39 -t db -f ~/git/python/pg_interaction/tcpdump/where_filename-20170307163203.dump_db.txt
	Для 1с:
python ~/git/python/pg_interaction/tcpdump/load_tcpdump.py -n 39 -t 1c -f ~/git/python/pg_interaction/tcpdump/where_filename-20170307170602.dump_1с.txt

6. Сравниваем запросом наподобие следующего:
select onec.time, db.time, onec.seq, db.time - onec.time as diff
from tcpdump as onec
left join tcpdump as db
    on db.seq = onec.seq and db.node = onec.node
where onec.node_type = '1c' and db.node_type = 'db'
order by diff desc;