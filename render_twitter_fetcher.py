#!/usr/bin/env python3
"""
==============================================================================
TWITTER DATA FETCHER - FULLY COMMENTED VERSION
==============================================================================
Purpose: Fetch tweets about Tanzania politics from Twitter API and store in database
Author: Your Name
Last Updated: January 6, 2026

This script:
1. Connects to Twitter API using Bearer Token
2. Searches for specific keywords (like "Samia Suluhu Hassan")
3. Extracts tweet data (text, likes, retweets, etc.)
4. Stores tweets in PostgreSQL database on Render
5. Runs automatically every 15 minutes via Render Cron Job

==============================================================================
"""

# ==============================================================================
# IMPORT REQUIRED LIBRARIES
# ==============================================================================

import psycopg2      # Library to connect to PostgreSQL database
import json          # Library to handle JSON data (convert Python objects to JSON)
import tweepy        # Library to interact with Twitter API
from datetime import datetime  # Library to work with dates and times
import os            # Library to read environment variables

# ==============================================================================
# CONFIGURATION - CREDENTIALS AND SETTINGS
# ==============================================================================

# DATABASE CONNECTION STRING
# This tells Python how to connect to your PostgreSQL database on Render
# Format: postgresql://username:password@host:port/database_name
DATABASE_URL = os.getenv(
    'DATABASE_URL',  # First, try to get from environment variable
    'postgresql://twitter_data_pygj_user:bBZRdS06QyhBHhBGWRtl4pkLjlaFO5MS@dpg-d5adn26r433s738bha50-a.singapore-postgres.render.com/twitter_data_pygj'  # If not found, use this default
)

# TWITTER API AUTHENTICATION
# This is your Bearer Token that proves you have permission to use Twitter API
# Twitter generates this when you create an app in their Developer Portal
BEARER_TOKEN = os.getenv(
    'TWITTER_BEARER_TOKEN',  # First, try to get from environment variable (more secure)
    'AAAAAAAAAAAAAAAAAAAAACi4zgEAAAAA0qATv6QklrjGbF8Pe9hdr1mavRk%3DTEwNMv92PRuxVfEbZfHxx098RMIbBzRyOMlNO1lsfXsA1GDr9U'  # If not found, use this default
)

# ==============================================================================
# FUNCTION 1: FETCH TWEETS FROM TWITTER
# ==============================================================================

def fetch_tweets():
    """
    This function connects to Twitter API and fetches tweets
    
    Steps:
    1. Create Twitter API client with authentication
    2. Search for tweets using keywords
    3. Extract relevant data from each tweet
    4. Return list of tweets with their data
    
    Returns:
        list: List of dictionaries, each containing tweet data
    """
    
    # Print start message with timestamp
    print(f"[{datetime.now()}] Starting Twitter fetch...")
    
    try:
        # ------------------------------------------------------------------
        # STEP 1: CREATE TWITTER CLIENT (Authentication)
        # ------------------------------------------------------------------
        
        # Create a connection to Twitter API using our Bearer Token
        # wait_on_rate_limit=True means: if we hit rate limit, wait automatically
        client = tweepy.Client(
            bearer_token=BEARER_TOKEN,      # Our authentication token
            wait_on_rate_limit=True         # Auto-wait if we exceed rate limits
        )
        
        print("✅ Connected to Twitter API")
        
        # ------------------------------------------------------------------
        # STEP 2: DEFINE SEARCH QUERIES
        # ------------------------------------------------------------------
        
        # List of keywords/phrases to search for
        # Twitter will find tweets containing these exact phrases
        queries = [
            "Samia Suluhu Hassan",  # Search for tweets mentioning the President
            # You can add more search terms here:
            # "Tundu Lissu",
            # "Tanzania politics",
            # "Tanzania 2026"
        ]
        
        # Empty list to store all tweets we find
        all_tweets = []
        
        # ------------------------------------------------------------------
        # STEP 3: SEARCH FOR EACH QUERY
        # ------------------------------------------------------------------
        
        # Loop through each search term
        for query in queries:
            print(f"🔍 Searching: {query}")
            
            try:
                # Ask Twitter API to search for recent tweets
                # This is the main API call that fetches data from Twitter
                response = client.search_recent_tweets(
                    query=query,              # What to search for
                    max_results=10,           # How many tweets to get (10-100)
                    
                    # TWEET FIELDS: What information about the tweet do we want?
                    tweet_fields=[
                        'created_at',         # When was it posted?
                        'public_metrics',     # Likes, retweets, replies counts
                        'lang',               # Language (en, sw, etc.)
                        'author_id'           # Who posted it?
                    ],
                    
                    # EXPANSIONS: Get additional related data
                    expansions=[
                        'author_id'           # Also get author information
                    ],
                    
                    # USER FIELDS: What information about the author do we want?
                    user_fields=[
                        'username',           # Twitter handle (@username)
                        'name',               # Display name
                        'verified',           # Blue checkmark?
                        'public_metrics'      # Follower count, etc.
                    ]
                )
                
                # ------------------------------------------------------------------
                # STEP 4: CHECK IF WE GOT ANY RESULTS
                # ------------------------------------------------------------------
                
                if not response.data:
                    # No tweets found for this search term
                    print(f"  ⚠️ No tweets found for '{query}'")
                    continue  # Skip to next search term
                
                # ------------------------------------------------------------------
                # STEP 5: BUILD A LOOKUP TABLE FOR AUTHORS
                # ------------------------------------------------------------------
                
                # Twitter returns user data separately from tweets
                # We need to match them up using author_id
                
                # Create empty dictionary to store user data
                users = {}
                
                # Check if response includes user data
                if response.includes and 'users' in response.includes:
                    # Create a dictionary: {user_id: user_object}
                    # This lets us quickly look up user info by ID
                    users = {u.id: u for u in response.includes['users']}
                
                # ------------------------------------------------------------------
                # STEP 6: PROCESS EACH TWEET
                # ------------------------------------------------------------------
                
                # Loop through each tweet we found
                for tweet in response.data:
                    
                    # --- Get Author Information ---
                    # Look up the author using their ID
                    author = users.get(tweet.author_id, None)
                    
                    # If we found the author, use their username
                    # If not, create a placeholder like "user_123456"
                    if author:
                        username = author.username
                    else:
                        username = f"user_{tweet.author_id}"
                    
                    # --- Build Tweet Data Dictionary ---
                    # Create a dictionary with all the tweet information we want to save
                    tweet_data = {
                        # BASIC INFORMATION
                        'id': tweet.id,                    # Unique tweet ID (number)
                        'text': tweet.text,                # Tweet content (text)
                        'created_at': tweet.created_at,    # When posted (datetime)
                        
                        # AUTHOR INFORMATION
                        'author_username': username,       # Who posted it (@username)
                        
                        # ENGAGEMENT METRICS (how popular is it?)
                        'retweet_count': tweet.public_metrics['retweet_count'],  # How many retweets
                        'like_count': tweet.public_metrics['like_count'],        # How many likes
                        
                        # METADATA (extra information)
                        'raw_data': {
                            'query': query,                # Which search found this tweet
                            'language': tweet.lang,        # Language code (en, sw, etc.)
                            'is_mock': False,              # This is real data, not mock
                            'source': 'twitter_api'        # Where it came from
                        }
                    }
                    
                    # Add this tweet to our list
                    all_tweets.append(tweet_data)
                
                # Print how many tweets we found for this search
                print(f"  ✅ Found {len(response.data)} tweets")
                
            except Exception as e:
                # If something goes wrong with this search, print error and continue
                print(f"  ❌ Error searching '{query}': {e}")
                continue  # Try next search term
        
        # ------------------------------------------------------------------
        # STEP 7: PRINT SUMMARY
        # ------------------------------------------------------------------
        
        # Print total tweets collected from all searches
        print(f"\n📊 Total collected: {len(all_tweets)} tweets")
        
        # ------------------------------------------------------------------
        # STEP 8: STORE TWEETS IN DATABASE
        # ------------------------------------------------------------------
        
        if all_tweets:
            # If we have any tweets, save them to database
            store_tweets(all_tweets)
        else:
            # If no tweets found, print message
            print("⚠️ No tweets to store")
        
        # Print completion message with timestamp
        print(f"[{datetime.now()}] Fetch completed!\n")
        
    except Exception as e:
        # If something goes wrong with the entire process, print error
        print(f"❌ Fatal error: {e}")

# ==============================================================================
# FUNCTION 2: STORE TWEETS IN DATABASE
# ==============================================================================

def store_tweets(tweets):
    """
    This function saves tweets to PostgreSQL database
    
    Args:
        tweets (list): List of tweet dictionaries to save
        
    Process:
    1. Connect to PostgreSQL database
    2. For each tweet, try to insert it
    3. If tweet already exists (duplicate ID), skip it
    4. Commit changes and close connection
    
    Returns:
        None (prints success/error messages)
    """
    
    try:
        # ------------------------------------------------------------------
        # STEP 1: CONNECT TO DATABASE
        # ------------------------------------------------------------------
        
        print("💾 Storing tweets in database...")
        
        # Create connection to PostgreSQL using our connection string
        conn = psycopg2.connect(DATABASE_URL)
        
        # Create cursor - this is what we use to execute SQL commands
        cursor = conn.cursor()
        
        # ------------------------------------------------------------------
        # STEP 2: INITIALIZE COUNTERS
        # ------------------------------------------------------------------
        
        # Keep track of how many tweets we successfully store
        stored_count = 0
        
        # Keep track of how many duplicates we skip
        duplicate_count = 0
        
        # ------------------------------------------------------------------
        # STEP 3: INSERT EACH TWEET
        # ------------------------------------------------------------------
        
        # Loop through each tweet we want to save
        for tweet in tweets:
            try:
                # --- Prepare SQL INSERT Statement ---
                
                # This SQL command inserts tweet data into 'tweets' table
                # ON CONFLICT (id) DO NOTHING means: if this tweet ID already exists, skip it
                cursor.execute("""
                    INSERT INTO tweets (
                        id,              -- Tweet ID (unique)
                        text,            -- Tweet content
                        author_username, -- Who posted it
                        created_at,      -- When posted
                        retweet_count,   -- Retweets
                        like_count,      -- Likes
                        raw_data         -- Extra data as JSON
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                """, (
                    # Values to insert (matching the order above)
                    tweet['id'],                    # From tweet dictionary
                    tweet['text'],                  # From tweet dictionary
                    tweet['author_username'],       # From tweet dictionary
                    tweet['created_at'],            # From tweet dictionary
                    tweet['retweet_count'],         # From tweet dictionary
                    tweet['like_count'],            # From tweet dictionary
                    json.dumps(tweet['raw_data'])   # Convert Python dict to JSON string
                ))
                
                # --- Check if Insert Was Successful ---
                
                # rowcount tells us how many rows were affected
                # If > 0, tweet was inserted successfully
                # If 0, tweet already existed (duplicate)
                if cursor.rowcount > 0:
                    stored_count += 1          # Increment success counter
                else:
                    duplicate_count += 1       # Increment duplicate counter
                
            except Exception as e:
                # If something goes wrong with this specific tweet, print error
                print(f"⚠️ Error storing tweet {tweet['id']}: {e}")
                continue  # Try next tweet
        
        # ------------------------------------------------------------------
        # STEP 4: SAVE CHANGES AND CLOSE CONNECTION
        # ------------------------------------------------------------------
        
        # Commit = permanently save all changes to database
        conn.commit()
        
        # Close cursor (no longer need it)
        cursor.close()
        
        # Close database connection
        conn.close()
        
        # ------------------------------------------------------------------
        # STEP 5: PRINT SUMMARY
        # ------------------------------------------------------------------
        
        print(f"✅ Stored: {stored_count} new tweets")
        
        if duplicate_count > 0:
            print(f"⏭️ Skipped: {duplicate_count} duplicates")
        
    except Exception as e:
        # If something goes wrong with database connection, print error
        print(f"❌ Database error: {e}")

# ==============================================================================
# MAIN PROGRAM ENTRY POINT
# ==============================================================================

if __name__ == "__main__":
    """
    This code runs when the script is executed directly
    
    It wraps our main function in error handling:
    - If user presses Ctrl+C, exit gracefully
    - If any other error occurs, print it
    """
    
    try:
        # Call the main function to fetch tweets
        fetch_tweets()
        
    except KeyboardInterrupt:
        # User pressed Ctrl+C to stop the script
        print("\n\n⚠️ Interrupted by user")
        
    except Exception as e:
        # Any other unexpected error
        print(f"\n\n❌ Unexpected error: {e}")

# ==============================================================================
# END OF SCRIPT
# ==============================================================================
