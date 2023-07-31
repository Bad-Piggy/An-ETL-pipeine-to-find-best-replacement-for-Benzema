query = """SELECT 
        t1.player_id,
        t1.player_name, 
        t1.sub_position, 
        t1.club_name, 
        t1.leagueName, 
        t1.height_in_cm, 
        t1.current_club_domestic_competition_id, 
        t1.sum_minutes_played, 
        t1.sum_goals, 
        t1.market_value_in_eur, 
        t1.age, 
        t1.foot
        FROM(SELECT 
        player_name, 
        country_of_citizenship,
        sub_position, 
        height_in_cm,
        current_club_domestic_competition_id,
        SUM(minutes_played) AS sum_minutes_played, 
        SUM(goals) AS sum_goals, 
        SUM(assists) AS sum_assists,
        SUM(games.away_club_goals),
        market_value_in_eur, 
        players.player_id,
        foot,
        competitions.name AS leagueName,
        clubs.name AS club_name,
        (2023 - CAST(extract(year FROM to_date(players.date_of_birth, 'yyyy')) as integer))AS age
    FROM players 
    INNER JOIN appearances ON players.player_id = appearances.player_id 
    INNER JOIN games ON appearances.game_id = games.game_id 
    INNER JOIN clubs ON appearances.player_current_club_id = clubs.club_id
    INNER JOIN competitions ON players.current_club_domestic_competition_id = competitions.competition_id
    GROUP BY player_name, sub_position, players.player_id, country_of_citizenship, height_in_cm, age, foot, leagueName, club_name, current_club_domestic_competition_id, market_value_in_eur
    HAVING SUM(minutes_played) >= 2000 
    ) AS t1"""
