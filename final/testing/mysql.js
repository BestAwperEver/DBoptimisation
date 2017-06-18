var mysql = require('mysql');
var cobj = {host:'radagast.asuscomm.com', user:'DBoptimization', password:'password', database:'climatic_data'};
var conn = mysql.createConnection(cobj);
var tabl = 'observations_small';
var i = process.argv[2];

var f1 = (pl, st, ds, de) => `select min(${pl}), avg(${pl}), max(${pl}) from ${tabl} where station_id=${st} and date>="${ds}" and date<"${de}" and ${pl} != -9999`;

var f2 = (pl, cn, ds, de) => `select station_id from ${tabl} where station_id%10=${cn} and date>="${ds}" and date<"${de}" and ${pl} != -9999 group by station_id having avg(${pl}) > (select avg(${pl}) from ${tabl} where station_id%10=${cn} and date>="${ds}" and date<"${de}" and ${pl} != -9999)`;

var f3 = (pl1, pl2, st, cn, ot, ds, de) => `select station_id from ${tabl}
where station_id%10=${cn} and station_id != ${st} and date>="${ds}" and date<"${de}" and ${pl1} != -9999 group by station_id, date
having avg(${pl1}) > (select avg(${pl1})-${ot} from ${tabl} where station_id=${st} and date>="${ds}" and date<"${de}" and ${pl1} != -9999) and
avg(${pl1}) < (select avg(${pl1})+${ot} from ${tabl} where station_id=${st} and date>="${ds}" and date<"${de}" and ${pl1} != -9999)`;

//desc select station_id from observations where station_id%10=4 and date>="2015-01-01" and date<"2015-01-15" and air_temperature != -9999 group by station_id having avg(air_temperature) > (select avg(air_temperature) from observations where station_id%10=4 and date>="2015-01-01" and date<"2015-01-15" and air_temperature != -9999);
//desc select station_id from observations where station_id%10=2 and date>="2015-01-01" and date<"2015-02-01" and air_temperature != -9999 group by station_id having avg(air_temperature) > (select avg(air_temperature) from observations use index (date_index) where station_id%10=2 and date>="2015-01-01" and date<"2015-02-01" and air_temperature != -9999);

//select station_id from observations where station_id%10=2 and date>="2015-01-01" and date<"2015-02-01" and air_temperature != -9999 group by station_id having avg(air_temperature) > (select avg(air_temperature) from observations where station_id%10=2 and date>="2015-01-01" and date<"2015-02-01" and air_temperature != -9999);


//select min(air_temperature), avg(air_temperature), max(air_temperature) from observations_small where station_id=10010 and date>="2015-01-01" and date<"2016-01-01" and air_temperature != -9999 use index();

//select station_id from observations where station_id%10=4 and date>="2015-01-01" and date<"2015-02-01" and dew_point != -9999 group by station_id having avg(dew_point) > (select avg(dew_point) from observations where station_id%10=4 and date>="2015-01-01" and date<"2015-02-01" and dew_point != -9999);
//select min(dew_point), avg(dew_point), max(dew_point) from observations where station_id=10010 and date>="2015-01-01" and date<"2016-01-01" and dew_point != -9999;

//select station_id from observations where station_id%10=4 and date>="2015-01-01" and date<"2015-01-15" and dew_point != -9999 group by station_id having avg(dew_point) > (select avg(dew_point) from observations where station_id%10=4 and date>="2015-01-01" and date<"2015-01-15" and dew_point != -9999);
//select min(dew_point), avg(dew_point), max(dew_point) from observations where station_id=10010 and date>="2015-01-01" and date<"2016-01-01" and dew_point != -9999;

//select min(air_temperature), avg(air_temperature), max(air_temperature) from observations where station_id=10010 and date>="2015-01-01" and date<"2016-01-01" and air_temperature != -9999;


//select station_id from observations use index() where station_id%10=2 and date>="2015-01-01" and date<"2015-01-15" and air_temperature != -9999 group by station_id having avg(air_temperature) > (select avg(air_temperature) from observations use index() where station_id%10=2 and date>="2015-01-01" and date<"2015-01-15" and air_temperature != -9999)




conn.connect();
var qr = '';
switch(i) {
	case '1':
		var pl = 'air_temperature';
		var st = '10010';
		var ds = '2015-01-01';
		var de = '2016-01-01';
		qr = f1(pl, st, ds, de);
		break;
	case '2':
		var pl = 'air_temperature';
		var cn = 2;  // Моделирование страны (1 - окончание id станции)
		var ds = '2015-01-01';
		var de = '2015-01-15';
		qr = f2(pl, cn, ds, de);
		break;
	case '3':
		var pl1 = 'air_temperature';
		var pl2 = 'air_temperature';
		var st = '10010';
		var cn = 1;  // Моделирование страны (1 - окончание id станции)
		var ot = 8;
		var ds = '2015-01-01';
		var de = '2015-01-15';
		qr = f3(pl1, pl2, st, cn, ot, ds, de);
		break;
	default:
		qr = 'select version()';
}
//console.log(qr);

for (let i = 0; i < 80000; ++i) {
	var qu = conn.query(qr, (error, results, fields) => {
		if (error) throw error;
		console.log(results);
	});
}



console.log(qu.sql);



conn.end();