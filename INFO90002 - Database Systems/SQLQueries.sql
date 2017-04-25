use wildlife;

-- Long Species Name
select * from species where length(species.title) >=
(select max(length(species.title))
from species);

-- Taxon F
select count(*) as "Number of Species" from taxon where taxon.title like "F%";

-- Text Search
select species.title as "Number of Species" 
	from species where species.title like "%Land%";

select count(species.title) as "Number of Species" 
	from species where LOCATE(BINARY ' Land ', CONCAT(' ', species.title, ' '));

-- Common Species
select species.id,species.title,species.taxon,count(anatar.species) from anatar inner join species
	on anatar.species=species.id
		group by anatar.species
			order by count(anatar.species) desc
				limit 5;


-- Capture Rate
select format(count(*)/
	(select count(*) from anatar where player is NULL)*100,2) as "Capture Rate in %"
		from anatar inner join species
			on anatar.species=species.id
				where species.title = "Mary River Cod";


-- Info About one Player
select anatar.id as "Anatar ID",species.title from anatar inner join species inner join player
	on anatar.species=species.id and anatar.player=player.id
		where player.username="Cook"
			order by anatar.id;
            

-- Top Players
select distinct player.id, player.username, count( distinct species.title) as "Number of Species" 
	from species inner join player inner join anatar
		on anatar.species=species.id and anatar.player=player.id
			group by player.username
				order by count(distinct species.title) desc
				limit 5;


-- Morning Catch
select player.id,player.username,count(player.id) as "Total Count" from player inner join anatar
	on anatar.player=player.id
		where whenSpawned< "2016-08-01 10:00:00"
			group by (player.username)
				order by count(whenSpawned) desc
					limit 5;

-- Team Players Counts
select team.title, count(player.id)as "Total Count" from team inner join player
	on team.id=player.team
		group by team.id
			order by count(player.id) desc;

-- Most Purchased Item
select purchase.item,item.title,sum(purchase.quantity) as "Sum Qty" from purchase inner join item
	on item.id=purchase.item
		where whenPurchased between "2016-08-10 00:00:00" and "2016-08-20 23:59:59"
			group by purchase.item
				order by sum(purchase.quantity) desc
					limit 1;
        

-- Top Earning in-app purchase
select purchase.item,item.title, sum(item.price*purchase.quantity) as "Income" 
	from purchase inner join item
		on item.id=purchase.item
			group by purchase.item
				order by sum(item.price*purchase.quantity) desc
					limit 1;

-- Small Spenders
select level,sum(item.price*purchase.quantity) as "Income" 
	from player inner join purchase inner join item
		on player.id=purchase.player and item.id=purchase.item
			group by player.level
				order by sum(item.price*purchase.quantity) asc
					limit 1;

-- Correlation
select medicine.points,sum(item.price*purchase.quantity) as Money from purchase inner join item inner join medicine
	on purchase.item=item.id and medicine.id=item.id
		union all
			select food.points,sum(item.price*purchase.quantity) as Money 
				from purchase inner join item inner join food
					on purchase.item=item.id and food.id=item.id
						group by food.points;

-- Near the City
-- distance in km = sqrt( (P1.latitude – P2.latitude)^2 + (P1.longitude – P2.longitude)^2 ) * 100

select player.username from player
where 3>sqrt(power(abs(player.latitude + 37.813889),2)+power(abs(player.longitude - 144.963367),2))*100;

-- Nearby Anatar
select count(*) as "Number of Anatars" from anatar
where 5>sqrt(power(abs(anatar.latitude - (select player.latitude from player where player.username="Reid")),2)
	+power(abs(anatar.longitude - (select player.longitude from player where player.username="Reid")),2))*100
and anatar.player is null;

-- Purchasing Food

select * from player
	where not exists
		(select * from food
			where not exists
				(select * from purchase
					where player.id=purchase.player
						and purchase.item=food.id));

-- Big Money Day
select DAYNAME(purchase.whenPurchased) as "Day",sum(item.price*purchase.quantity) as "Income"
	from purchase inner join item
		on purchase.item=item.id
			group by DAYNAME(purchase.whenPurchased)
				order by sum(item.price*purchase.quantity) desc
					limit 1;
        
-- Ready to Battle
select player.username, sum(anatar.power) as "Power" from player inner join anatar
	on player.id = anatar.player
		where player.username="Cook" 
			or player.username="Hughes"
				group by player.username;

