import pytest
from datetime import date

import crud
from database import SessionLocal

test_date = date(2024,4,1)

@pytest.fixture(scope="function")
def db_session():
    session = SessionLocal()
    yield session
    session.close()

def test_get_player(db_session):
    player = crud.get_player(db_session, player_id = 1001)
    assert player.player_id == 1001

def test_get_players(db_session):
    players = crud.get_players(db_session, skip=0, limit=10000, min_last_changed_date=test_date)
    assert len(players) == 1018

def test_get_players_by_name(db_session):
    players = crud.get_players(db_session, first_name="Bryce", last_name="Young")
    assert len(players) == 1
    assert players[0].player_id == 2009

def test_get_all_performanncs(db_session):
    performances = crud.get_performances(db_session, skip=0, limit=18000)
    assert len(performances) == 17306

def test_get_new_performances(db_session):
    performances = crud.get_performances(db_session, skip=0, limit=10000,min_last_changed_date=test_date)
    assert len(performances) == 2711

def test_get_league(db_session):
    league = crud.get_league(db_session, league_id = 5002)
    assert league.league_id == 5002
    assert len(league.teams) == 8

def test_get_leagues(db_session):
    leagues = crud.get_leagues(db_session, skip=0, limit=10000, min_last_changed_date=test_date)
    assert len(leagues) == 5

def test_get_teams(db_session):
    teams = crud.get_teams(db_session, skip=0, limit=10000, min_last_changed_date=test_date)
    assert len(teams) == 20

def test_get_teams_for_one_league(db_session):
    teams = crud.get_teams(db_session, league_id=5001)
    assert len(teams) == 12
    assert teams[0].league_id == 5001

def test_get_team_players(db_session):
    first_team = crud.get_teams(db_session, skip=0, limit=1000, min_last_changed_date=test_date)[0]
    assert len(first_team.players) == 7

def test_get_player_count(db_session):
    player_count = crud.get_player_count(db_session)
    assert player_count == 1018

def test_get_team_count(db_session):
    team_count = crud.get_team_count(db_session)
    assert team_count == 20

def test_get_league_count(db_session):
    league_count = crud.get_league_count(db_session)
    assert league_count == 5