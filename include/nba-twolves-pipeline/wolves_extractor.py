#!/usr/bin/env python3
"""
Minnesota Timberwolves Data Extractor

This script extracts the Minnesota Timberwolves NBA schedule and per-player box scores
for the specified season, saving the results as CSV files in the data/raw directory.

Usage:
    python wolves_extractor.py

Environment Variables:
    TEAM_ID: NBA team ID (default: 1610612750 for Timberwolves)
    SEASON: NBA season in format YYYY-YY (default: 2024-25)
"""

import os
import time
import pandas as pd
from pathlib import Path
from datetime import date
from nba_api.stats.endpoints import TeamGameLog, BoxScoreTraditionalV2


def fetch_games(team_id, season):
    """
    Fetch all games for the specified team and season.
    
    Args:
        team_id (str): The NBA team ID
        season (str): The NBA season in format YYYY-YY
        
    Returns:
        DataFrame: A pandas DataFrame containing the game data
    """
    print(f"Fetching games for team ID {team_id} in {season} season...")
    
    try:
        # Call the TeamGameLog endpoint
        game_log = TeamGameLog(team_id=team_id, season=season)
        games_df = game_log.get_data_frames()[0]
        print(f"Successfully fetched game log with {len(games_df)} games")
        
        # Keep only necessary columns and rename for clarity
        games_df = games_df.rename(columns={
            'Game_ID': 'game_id',
            'GAME_DATE': 'game_date',
            'MATCHUP': 'matchup',
            'WL': 'result',
            'W': 'wins',
            'L': 'losses',
            'PTS': 'team_points',
            'FG_PCT': 'team_fg_pct',
            'FT_PCT': 'team_ft_pct',
            'REB': 'team_rebounds',
            'AST': 'team_assists',
            'TOV': 'team_turnovers'
        })
        
        return games_df
    except Exception as e:
        print(f"ERROR in fetch_games: {e}")
        return pd.DataFrame()


def fetch_boxscores(game_ids):
    """
    Fetch box scores for a list of game IDs.
    
    Args:
        game_ids (list): List of NBA game IDs
        
    Returns:
        DataFrame: A pandas DataFrame containing player stats from all games
    """
    print(f"Fetching box scores for {len(game_ids)} games...")
    
    all_player_stats = []
    
    for i, game_id in enumerate(game_ids):
        print(f"Processing game {i+1}/{len(game_ids)}: {game_id}")
        
        try:
            # Call the BoxScoreTraditionalV2 endpoint
            box_score = BoxScoreTraditionalV2(game_id=game_id)
            player_stats = box_score.get_data_frames()[0]
            
            # Add game_id to the player stats
            player_stats['game_id'] = game_id
            
            # Append to the list
            all_player_stats.append(player_stats)
            print(f"  ✓ Game {i+1}: Added {len(player_stats)} player records")
            
            # Sleep to respect rate limits (at least 0.6s between calls)
            if i < len(game_ids) - 1:
                time.sleep(0.6)
        except Exception as e:
            print(f"  ✗ ERROR processing game {game_id}: {e}")
    
    # Concatenate all player stats into a single DataFrame
    if all_player_stats:
        print(f"Concatenating {len(all_player_stats)} game box scores...")
        player_stats_df = pd.concat(all_player_stats, ignore_index=True)
        
        # Rename columns for clarity
        player_stats_df = player_stats_df.rename(columns={
            'PLAYER_ID': 'player_id',
            'PLAYER_NAME': 'player_name',
            'TEAM_ID': 'team_id',
            'TEAM_ABBREVIATION': 'team_abbr',
            'MIN': 'minutes',
            'PTS': 'points',
            'FGM': 'fg_made',
            'FGA': 'fg_attempts',
            'FG_PCT': 'fg_pct',
            'FG3M': 'fg3_made',
            'FG3A': 'fg3_attempts',
            'FG3_PCT': 'fg3_pct',
            'FTM': 'ft_made',
            'FTA': 'ft_attempts',
            'FT_PCT': 'ft_pct',
            'OREB': 'offensive_rebounds',
            'DREB': 'defensive_rebounds',
            'REB': 'total_rebounds',
            'AST': 'assists',
            'STL': 'steals',
            'BLK': 'blocks',
            'TO': 'turnovers'
        })
        
        return player_stats_df
    
    print("WARNING: No player stats data collected!")
    return pd.DataFrame()


def main():
    """
    Main function to extract and save Timberwolves data.
    """
    # Get team ID and season from environment variables or use defaults
    team_id = os.environ.get('TEAM_ID', '1610612750')  # Default: Timberwolves
    season = os.environ.get('SEASON', '2024-25')
    
    print(f"Starting extraction with TEAM_ID={team_id}, SEASON={season}")
    
    # Fetch games data
    games_df = fetch_games(team_id, season)
    
    if games_df.empty:
        print("ERROR: No games data retrieved. Aborting.")
        return
    
    # Extract game IDs for box score fetching
    game_ids = games_df['game_id'].tolist()
    print(f"Found {len(game_ids)} game IDs to process")
    
    # Fetch player stats for all games
    player_stats_df = fetch_boxscores(game_ids)
    
    if player_stats_df.empty:
        print("ERROR: No player stats data retrieved. Aborting.")
        return
    
    # Create output directory path with today's date (YYYYMMDD)
    today_str = date.today().strftime('%Y%m%d')
    output_dir = Path(f"data/raw/{today_str}")
    
    # Print current working directory for debugging
    print(f"Current working directory: {os.getcwd()}")
    print(f"Output directory to create: {output_dir}")
    
    # Create directories if they don't exist
    try:
        output_dir.mkdir(parents=True, exist_ok=True)
        print(f"Created output directory: {output_dir}")
    except Exception as e:
        print(f"ERROR creating directory {output_dir}: {e}")
        return
    
    # Write CSVs without index
    games_csv_path = output_dir / "games.csv"
    player_stats_csv_path = output_dir / "player_stats.csv"
    
    print(f"Writing games data to {games_csv_path}")
    try:
        games_df.to_csv(games_csv_path, index=False)
        print(f"✓ Wrote {len(games_df)} rows to {games_csv_path}")
    except Exception as e:
        print(f"ERROR writing games CSV: {e}")
    
    print(f"Writing player stats data to {player_stats_csv_path}")
    try:
        player_stats_df.to_csv(player_stats_csv_path, index=False)
        print(f"✓ Wrote {len(player_stats_df)} rows to {player_stats_csv_path}")
    except Exception as e:
        print(f"ERROR writing player stats CSV: {e}")


if __name__ == "__main__":
    main()

